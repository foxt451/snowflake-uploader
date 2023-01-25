import { BasicCrawlingContext } from 'crawlee';
import snowflake from 'snowflake-sdk';

export type UploadBatchData = {
    datasetId: string;
    offset: number;
    length: number;
};

export type CrawlerContext = BasicCrawlingContext & {
    snowflakeConnectionPool: ReturnType<typeof snowflake.createPool>;
    datasetId: string;
};

export enum LABEL {
    EXTRACT = 'extract',
    LOAD = 'load',
}

export type Input = {
    synchronizeSchema?: { [key: string]: string };
    dataLossConfirmation: boolean;
    overwrite: boolean,
    datasetId: string;
    username: string;
    account: string;
    password: string;
    warehouse: string;
    database: string
};