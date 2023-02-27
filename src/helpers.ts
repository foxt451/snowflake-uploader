import { log } from 'apify';
import snowflake from 'snowflake-sdk';

type PromisifyParams = {
    snowflakeConnection: snowflake.Connection;
    statement: string;
    verb: string;
    rowCallback?: (row: any) => void;
    closedCallback?: () => void;
};

export const promisifySnoflakeExecute = ({ statement, verb, closedCallback, rowCallback, snowflakeConnection }: PromisifyParams): Promise<void> => {
    return new Promise<void>((res, rej) =>
        snowflakeConnection.execute({
            sqlText: statement,
            complete: function (err, statement) {
                if (err) {
                    rej(new Error(`Failed to execute ${verb} statement due to the following error: ${err.message}`));
                }
                const stream = statement.streamRows();
                stream.on('error', function (err) {
                    rej(new Error(`Failed to execute ${verb} statement due to the following error: ${err.message}`));
                })
                stream.on('data', function (row) {
                    rowCallback?.(row);
                });
                stream.on('end', function () {
                    closedCallback?.();
                    res();
                });
            },
        })
    );
};

export const sequentialRetry = async <TResult>(fn: () => Promise<TResult>, retries: number = 5, contextMsg?: string): Promise<TResult> => {
    try {
        return await fn();
    } catch (e: any) {
        if (retries > 0) {
            log.warning(`Retrying after error: ${e.message}. Retries left: ${retries}`);
            if (contextMsg) {
                log.warning(`${contextMsg}`);
            }
            return await sequentialRetry(fn, retries - 1);
        }
        throw e;
    }
};
