const queries = require('./sql/v2/queries');
const crypto = require('crypto');
const schemaVersion = 2;
const serializationError = '40001';
const pgBootNS = require("pg-boot");
const productName = "Q";

module.exports = class PgQueue {

    name;
    pages;
    #subscriber;
    #writerPG;
    #queries;
    #pgBoot;
    #serializeTransactionMode;
    #dqFunctionName;
    #ackFunctionName;

    constructor(name, writerPG, pages = 20, subscriber = { "name": "Primary", "messagesPerBatch": 1 }) {
        if (Number.isNaN(pages) || pages > 20 || pages < 3) throw new Error("Invalid parameter pages, should be between 3 to 20");
        if (writerPG == undefined) throw new Error("Invalid parameter writerPG, should be a valid database connection");
        if (name == undefined || typeof name != "string" || name.length < 3) throw new Error("Invalid parameter name, should be a string with length between 3 to 20");
        if (subscriber.name == undefined || subscriber.name.length > 32 || subscriber.name.length == 0) throw new Error("Invalid subscriber information 'name' must be string from 1 to 32 chars long.");
        if (!Number.isNaN(subscriber.messagesPerBatch) && (subscriber.messagesPerBatch > 1000 || Math.ceil(Math.abs(subscriber.messagesPerBatch) || subscriber.messagesPerBatch < 1) !== subscriber.messagesPerBatch)) throw new Error("Invalid subscriber information 'messagesPerBatch' must be whole number from 1 to 1000.");
        if (Number.isNaN(subscriber.messagesPerBatch)) subscriber.messagesPerBatch = 1;

        this.#writerPG = writerPG;
        this.#subscriber = subscriber;
        this.#serializeTransactionMode = new this.#writerPG.$config.pgp.txMode.TransactionMode({
            tiLevel: this.#writerPG.$config.pgp.txMode.isolationLevel.serializable,
            readOnly: false,
            deferrable: false
        });
        this.pages = pages;
        this.name = crypto.createHash('md5').update(name).digest('hex');

        this.tryEnque = this.tryEnque.bind(this);
        this.tryDeque = this.tryDeque.bind(this);
        this.tryAcknowledge = this.tryAcknowledge.bind(this);
        this.deleteSubscriber = this.deleteSubscriber.bind(this);
        this.#initialize = this.#initialize.bind(this);

        this.#pgBoot = new pgBootNS.PgBoot(productName + name);
    }

    #initialize = async (version) => {
        return this.#pgBoot.checkVersion(this.#writerPG, version, async (transaction, dbVersion) => {
            const ExistingQuePagesFunctionName = "Pages-" + this.name;
            const QTableName = "Q-" + this.name;
            const CursorTableName = "C-" + this.name;
            const CursorTablePKName = CursorTableName + "-PK";
            const SubscriberTableName = "S-" + this.name;
            const SubscriberRegistrationFunctionName = "SR-" + this.name;
            this.#dqFunctionName = "DQ-" + this.name;
            this.#ackFunctionName = "ACK-" + this.name;
            this.#queries = queries(CursorTableName, QTableName, ExistingQuePagesFunctionName, SubscriberTableName);
            let results;
            switch (dbVersion) {
                case -1: //First time install
                    for (let idx = 0; idx < this.#queries.Schema1.length; idx++) {
                        let step = this.#queries.Schema1[idx];
                        let stepParams = {
                            "pagesfunctionname": ExistingQuePagesFunctionName,
                            "totalpages": this.pages,
                            "qtablename": QTableName,
                            "cursortablename": CursorTableName,
                            "subscriberstablename": SubscriberTableName,
                            "cursorprimarykeyconstraintname": CursorTablePKName,
                            "gctriggerfunctionname": "GCFunc-" + this.name,
                            "gctriggername": "GC-" + this.name,
                            "dequeuefunctionname": this.#dqFunctionName,
                            "acknowledgepayloadfunctionname": this.#ackFunctionName,
                            "trydequeuefunctionname": "TRY-DQ-" + this.name,
                            "tryacknowledgepayloadfunctionname": "TRY-ACK-" + this.name,
                            "subscriberregistrationfunctionname": SubscriberRegistrationFunctionName
                        };
                        await transaction.none(step, stepParams);
                    };
                    results = await transaction.func(SubscriberRegistrationFunctionName, [this.#subscriber.name, this.#subscriber.messagesPerBatch]);
                    this.#subscriber.messagesPerBatch = results[0][SubscriberRegistrationFunctionName];
                    break;
                case 1: //Version 1
                    //TODO Update
                    break;
                case version://Same version 
                    results = await transaction.func(ExistingQuePagesFunctionName);
                    this.pages = results[0][ExistingQuePagesFunctionName];
                    results = await transaction.func(SubscriberRegistrationFunctionName, [this.#subscriber.name, this.#subscriber.messagesPerBatch]);
                    this.#subscriber.messagesPerBatch = results[0][SubscriberRegistrationFunctionName];
                    break;
                default:
                    console.error("Unknown schema version " + dbVersion);
                    break;
            };
        });
    }

    async tryEnque(payloads) {
        if (Array.isArray(payloads) === false || payloads.length > 40000) throw new Error("Invalid parameter payloads, expecting array of payloads not more than 40k");

        await this.#initialize(schemaVersion);

        await this.#writerPG.tx((transaction) => {
            payloads.map((p) => transaction.none(this.#queries.Enqueue, [p]));
            return transaction.batch;
        });
        //TODO Handle QUE FULL scenario(When one cursor is lagging and other cursor is fastest)
        return;
    }

    async tryDeque(messageAcquiredTimeout = 3600, retry = 10) {
        retry = parseInt(retry);
        if (Number.isNaN(retry)) throw new Error("Invalid retry count " + retry);
        messageAcquiredTimeout = parseInt(messageAcquiredTimeout);
        if (Number.isNaN(messageAcquiredTimeout)) throw new Error("Invalid timeout " + messageAcquiredTimeout + ", excepting seconds count.");

        await this.#initialize(schemaVersion);

        let message;
        while (retry > 0) {
            try {
                let mode = this.#serializeTransactionMode;
                message = await this.#writerPG.tx({ mode }, transaction => {
                    return transaction.func(this.#dqFunctionName, [this.#subscriber.name, messageAcquiredTimeout]);
                });
                retry = 0;
            }
            catch (err) {
                if (err.code === serializationError)//Serialization error
                {
                    continue;
                }
                else {
                    throw err;
                }
            }
            finally {
                retry--;
            }
        }
        if (message == undefined || message.length <= 0) return undefined;
        return message.map(m => ({
            "Id": { "P": m.P, "S": m.S },
            "AckToken": { "T": m.T, "C": m.C },
            "Payload": m.M
        }));

    }

    async tryAcknowledge(tokens, retry = 10) {
        retry = parseInt(retry);
        if (Number.isNaN(retry)) throw new Error("Invalid retry count " + retry);

        await this.#initialize(schemaVersion);

        let message;
        while (retry > 0) {
            try {
                let mode = this.#serializeTransactionMode;
                message = await this.#writerPG.tx({ mode }, transaction => {
                    return transaction.any('SELECT * FROM $1:name($2:json)', [this.#ackFunctionName, tokens]);
                });
                retry = 0;
            }
            catch (err) {
                if (err.code === serializationError)//Serialization error
                {
                    continue;
                }
                else {
                    throw err;
                }
            }
            finally {
                retry--;
            }
        }
        return {
            "retry": (message == undefined),
            "pending": (message == undefined) ? undefined : message[0][this.#ackFunctionName]
        };
    }

    async deleteSubscriber() {
        await this.#initialize(schemaVersion);

        let mode = this.#serializeTransactionMode;
        await this.#writerPG.tx({ mode }, async transaction => {
            await transaction.none(this.#queries.DeleteSubCursor, [this.#subscriber.name]);
            await transaction.none(this.#queries.DeleteSub, [this.#subscriber.name]);
        });

        return true;
    }
}
//Why cant this be done with pg-cursors? Cursors are session lived object not available across connections(Even WITH HOLD).