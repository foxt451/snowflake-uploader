import { Actor, log } from 'apify';
import { BasicCrawler, BasicCrawlingContext } from 'crawlee';
import snowflake from 'snowflake-sdk';
import fs, { createWriteStream } from 'fs';
import { CrawlerContext, Input } from './types.js';
import { promisifySnoflakeExecute } from './helpers.js';

await Actor.init();

const input = (await Actor.getInput<Input>())!;
const { synchronizeSchema, datasetId, username, account, password, warehouse, database, overwrite, dataLossConfirmation } = input;

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
        statement: `DROP TABLE IF EXISTS APIFY.PUBLIC.EANS;`,
        verb: 'DROP',
    });
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `CREATE TABLE APIFY.PUBLIC.EANS (${Object.entries(synchronizeSchema)
            .map(([key, value]) => `${key} ${value}`)
            .join(', ')});`,
        verb: 'CREATE',
    });
}

const outputLocation = './dataset.json';

// const writer = createWriteStream(outputLocation);

// await Actor.apifyClient.httpClient
//     .call({
//         url: `https://api.apify.com/v2/datasets/${datasetId}/items?token=${Actor.apifyClient.token}&format=json`,
//         method: 'GET',
//         responseType: 'stream',
//     })
//     .then((response) => {
//         return new Promise((resolve, reject) => {
//             response.data.pipe(writer);
//             let error: Error | null = null;
//             writer.on('error', (err) => {
//                 error = err;
//                 writer.close();
//                 reject(err);
//             });
//             writer.on('close', () => {
//                 if (!error) {
//                     resolve(true);
//                 }
//             });
//         });
//     });

await promisifySnoflakeExecute({
    snowflakeConnection,
    statement: `PUT file:///Users/foxt451/Code/snowflake-uploader/dataset.json @APIFY.PUBLIC.%EANS overwrite=TRUE;`,
    verb: 'PUT',
});

if (overwrite && dataLossConfirmation) {
    await promisifySnoflakeExecute({
        snowflakeConnection,
        statement: `DELETE FROM APIFY.PUBLIC.EANS;`,
        verb: 'DELETE',
    });
}

await promisifySnoflakeExecute({
    snowflakeConnection,
    statement: `COPY INTO APIFY.PUBLIC.EANS FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;`,
    verb: 'COPY',
});

await Actor.exit();
