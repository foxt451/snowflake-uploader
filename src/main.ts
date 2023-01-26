import { Actor, log } from 'apify';
import { BasicCrawler, BasicCrawlingContext } from 'crawlee';
import snowflake from 'snowflake-sdk';
import fs, { createWriteStream } from 'fs';
import { CrawlerContext, Input } from './types.js';
import streamJson from 'stream-json';
import { promisifySnoflakeExecute } from './helpers.js';
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
    account,
    password,
    warehouse,
    database,
    overwrite,
    dataLossConfirmation,
} = input;

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

if (synchronizeSchema && dataLossConfirmation) {
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `DROP TABLE IF EXISTS ${tableName};`,
        verb: 'DROP',
    });
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `CREATE TABLE ${tableName} (${Object.entries(synchronizeSchema)
            .map(([key, value]) => `"${key}" ${value}`)
            .join(', ')});`,
        verb: 'CREATE',
    });
}

const outputLocation = '/tmp/dataset.json';

const writer = createWriteStream(outputLocation);

const transformKeyFunc = transformJsonKeyFunction && new Function('key', transformJsonKeyFunction);

let transformedCount = 0;
const TRANSFORMED_LOG_INTERVAL = 100;
await Actor.apifyClient.httpClient
    .call({
        url: `https://api.apify.com/v2/datasets/${datasetId}/items?token=${Actor.apifyClient.token}&format=json&limit=100`,
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
                    if (!value || !flattenJson) {
                        return data;
                    }
                    const newValue = transformKeyFunc ? Object.fromEntries(Object.entries(value).map(([k, v]) => [transformKeyFunc(k), v])) : value;
                    return {
                        ...data,
                        value: newValue,
                    };
                },
                (data) => {
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
const tableStageName = `@${splitTableName[0]}.${splitTableName[1]}.%${splitTableName[2]}`;
let counter = 0;
const LOG_COPY_INTERVAL = 100;
await promisifySnoflakeExecute({
    snowflakeConnection,
    statement: `PUT file://${outputLocation} ${tableStageName} overwrite=TRUE;`,
    verb: 'PUT',
    rowCallback: () => {
        counter++;
        if (counter % LOG_COPY_INTERVAL === 0) {
            log.info(`Copied into database ${counter} rows`);
        }
    },
});

if (overwrite && dataLossConfirmation) {
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `DELETE FROM ${tableName};`,
        verb: 'DELETE',
    });
}

await promisifySnoflakeExecute({
    snowflakeConnection,
    statement: `COPY INTO ${tableName} FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;`,
    verb: 'COPY',
});

await Actor.exit();
