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
                    console.log(err)
                    rej(new Error(`Failed to execute ${verb} statement due to the following error: ${err.message}`));
                }
                var stream = statement.streamRows();
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
