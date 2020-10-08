// Hand written type definitions for pg-que 0.0.4
import * as pg from 'pg-promise';
interface PayloadId {
    T: string;
    S: bigint;
}
interface Payload {
    Id: PayloadId;
    AckToken: number;
    Payload: any;
}

export default class PgQueue {
    constructor(name: string, readerPG: pg.IDatabase<any>, writerPG: pg.IDatabase<any>, cleanQAfter?: number);
    enque(payloads: any[]): Promise<void>;
    tryDeque(messageAcquiredTimeout?: number, retry?: number): Promise<Payload>;
    tryAcknowledge(token:number, retry?: number): Promise<Boolean>;
}