// Hand written type definitions for pg-que 0.0.4

interface PayloadId {
    T: string;
    S: bigint;
}
interface Payload {
    Id: PayloadId;
    AckToken: number;
    Payload: any;
}

declare class PgQueue {
    constructor(name: string, readerPG: any, writerPG: any, schema?: string, cleanQAfter?: number);
    enque(payloads: any[]): Promise<void>;
    tryDeque(messageAcquiredTimeout?: number, retry?: number): Promise<Payload>;
    tryAcknowledge(token:number, retry?: number): Promise<Boolean>;
    dispose(): void;
}

export default PgQueue