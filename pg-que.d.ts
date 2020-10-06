// Hand written type definitions for pg-que 0.0.4

interface PayloadId {
    T: string,
    S: bigint
}
interface Payload {
    Id: PayloadId,
    AckToken: integer,
    Payload: any
}

declare class PgQueue {

    constructor(name: string, readerPG: any, writerPG: any, schema: string, cleanQAfter: integer);
    enque(payloads: Array<any>): Promise<void>;
    tryDeque(messageAcquiredTimeout: integer, retry: integer): Promise<Payload>;
    tryAcknowledge(token: integer, retry: integer): Promise<Boolean>;
    dispose(): void;
}
