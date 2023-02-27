import { Actor, log } from 'apify';
import { BasicCrawler, BasicCrawlingContext, sleep } from 'crawlee';
import snowflake from 'snowflake-sdk';
import fs, { createWriteStream } from 'fs';
import { CrawlerContext, Input } from './types.js';
import streamJson from 'stream-json';
import { promisifySnoflakeExecute, sequentialRetry } from './helpers.js';
import stringerModule from 'stream-json/Stringer.js';
import disassemblerModule from 'stream-json/Disassembler.js';
import streamChain from 'stream-chain';
import flat from 'flat';
import streamArrayModule from 'stream-json/streamers/StreamArray.js';
// commonJS modules
const { flatten } = flat;
const { streamArray } = streamArrayModule;
const { chain } = streamChain;
const { parser } = streamJson;
const { stringer } = stringerModule;
const { disassembler } = disassemblerModule;

await Actor.init();

const input = (await Actor.getInput<Input>())!;
const {
    synchronizeSchema,
    transformJsonKeyFunction,
    datasetId,
    flattenJson,
    tableName,
    username,
    fileUploadRetries = 5,
    account,
    password,
    warehouse,
    limit,
    database,
    overwrite,
    transformJsonDataFunction,
    stage,
    dataLossConfirmation,
} = input;
const selectedDatasetId = datasetId ?? input.resource?.defaultDatasetId;
if (!selectedDatasetId) {
    throw new Error('No datasetId provided');
}

const snowflakeConnection = snowflake.createConnection({
    username,
    account,
    password,
    database,
    warehouse,
});

await new Promise<void>((res, rej) => {
    snowflakeConnection.connect(function (err) {
        if (err) {
            rej(new Error('Unable to connect: ' + err.message));
        } else {
            log.info('Successfully connected to Snowflake.');
            res();
        }
    });
});

if (synchronizeSchema) {
    if (!dataLossConfirmation) {
        throw new Error('You must set "dataLossConfirmation" to true if you want to use "synchronizeSchema"!');
    }
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `DROP TABLE IF EXISTS ${tableName};`,
        verb: 'DROP',
    });
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `CREATE TABLE ${tableName} (${synchronizeSchema.map(({ key, value }) => `"${key}" ${value}`).join(', ')});`,
        verb: 'CREATE',
    });
    log.info('Synchronized schema');
}

const outputLocation = '/tmp/dataset.json';

const writer = createWriteStream(outputLocation);

const transformKeyFunc = transformJsonKeyFunction && new Function('key', transformJsonKeyFunction);
const transformDataFunc = transformJsonDataFunction && new Function('value', transformJsonDataFunction);

let transformedCount = 0;
const TRANSFORMED_LOG_INTERVAL = 100;
const TRANSFORMED_VIEW_INTERVAL = 2000;
// using underlying http client instead of apify client, because response streaming is used, which is not supported
// out of the box by apify client;
// using response streaming because in case of huge datasets, it might not be possible to load all data into memory
await Actor.apifyClient.httpClient
    .call({
        url: `https://api.apify.com/v2/datasets/${selectedDatasetId}/items?token=${Actor.apifyClient.token}&format=json${
            limit ? `&limit=${limit}` : ''
        }`,
        method: 'GET',
        responseType: 'stream',
    })
    .then((response) => {
        return new Promise((resolve, reject) => {
            chain([
                response.data,
                parser(),
                streamArray(),
                (data) => {
                    const value = data.value;
                    if (!value || !flattenJson) {
                        return data;
                    }
                    return {
                        ...data,
                        value: flatten(value),
                    };
                },
                (data) => {
                    const value = data.value;
                    if (!value || !transformKeyFunc) {
                        return data;
                    }
                    const newValue = Object.fromEntries(Object.entries(value).map(([k, v]) => [transformKeyFunc(k), v]));
                    return {
                        ...data,
                        value: newValue,
                    };
                },
                (data) => {
                    const value = data.value;
                    if (!value || !transformDataFunc) {
                        return data;
                    }
                    const newValue = transformDataFunc(value);
                    return {
                        ...data,
                        value: newValue,
                    };
                },
                (data) => {
                    if (transformedCount % TRANSFORMED_VIEW_INTERVAL === 0) {
                        log.info(`Transformed object looks like: ${JSON.stringify(data.value, null, 2)}`);
                    }
                    transformedCount++;
                    if (transformedCount % TRANSFORMED_LOG_INTERVAL === 0) {
                        log.info(`Transformed ${transformedCount} rows`);
                    }
                    return data;
                },
                (data) => {
                    return data.value ?? null;
                },
                disassembler(),
                stringer(),
                writer,
            ]);
            let error: Error | null = null;
            writer.on('error', (err) => {
                error = err;
                writer.close();
                reject(err);
            });
            writer.on('close', () => {
                if (!error) {
                    resolve(true);
                }
            });
        });
    });

log.info('Downloaded and transformed the dataset');

const splitTableName = tableName.split('.');
const tableStageName = stage || `@${splitTableName[0]}.${splitTableName[1]}.%${splitTableName[2]}`;
let counter = 0;
const LOG_COPY_INTERVAL = 100;
await promisifySnoflakeExecute({
    snowflakeConnection,
    statement: `PUT file://${outputLocation} ${tableStageName} overwrite=TRUE;`,
    verb: 'PUT',
});

log.info('Uploaded data file to stage');

// sleep some time snowflake recognizes the file
const POST_UPLOAD_SLEEP_MS = 5000;
log.info(`Waiting ${POST_UPLOAD_SLEEP_MS} ms after uploading file to stage...`);
await sleep(POST_UPLOAD_SLEEP_MS);

if (overwrite) {
    if (!dataLossConfirmation) {
        throw new Error('You must set "dataLossConfirmation" to true if you want to use "synchronizeSchema"!');
    }
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `DELETE FROM ${tableName};`,
        verb: 'DELETE',
    });
    log.info('Deleted previous data');
}

await sequentialRetry(() => promisifySnoflakeExecute({
    snowflakeConnection,
    statement: `COPY INTO ${tableName} FROM ${tableStageName} FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE PURGE = TRUE;`,
    verb: 'COPY',
}), fileUploadRetries, 'Note: there is an active issue at snowflake`s driver that sometimes causes errors during large file uploads. It is often resolved after a couple of retries.');

await Actor.exit();
