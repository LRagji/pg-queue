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
interface SubscriberInfo {
    "name": String,
    "messagesPerBatch": Integer
}

export default class PgQueue {
    constructor(name: string, writerPG: pg.IDatabase<any>, pages?: number, subscriber?: SubscriberInfo);
    enque(payloads: any[]): Promise<void>;
    tryDeque(messageAcquiredTimeout?: number, retry?: number): Promise<Payload>;
    tryAcknowledge(token: number, retry?: number): Promise<Boolean>;
    tryDeleteSubscriber(): Promise<Boolean>;
}