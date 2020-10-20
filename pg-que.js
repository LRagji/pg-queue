const queries = require('./sql/v2/queries');
const crypto = require('crypto');
const schemaVersion = 2;
const serializationError = '40001';
const pgBootNS = require("pg-boot");
const productName = "Q";

module.exports = class PgQueue {

    name;
    pages;
    #cursorId;
    #readerPG;
    #writerPG;
    #queries;
    #pgBoot;
    #serializeTransactionMode;

    constructor(name, readerPG, writerPG, pages = 20) {
        if (Number.isNaN(pages) || pages > 20 || pages < 3) throw new Error("Invalid parameter pages, should be between 3 to 20");
        if (readerPG == undefined) throw new Error("Invalid parameter readerPG, should be a valid database connection");
        if (writerPG == undefined) throw new Error("Invalid parameter writerPG, should be a valid database connection");
        if (name == undefined || typeof name != "string" || name.length < 3) throw new Error("Invalid parameter name, should be a string with length between 3 to 20");

        this.#readerPG = readerPG;
        this.#writerPG = writerPG;
        this.#cursorId = 1;
        this.#serializeTransactionMode = new this.#writerPG.$config.pgp.txMode.TransactionMode({
            tiLevel: this.#writerPG.$config.pgp.txMode.isolationLevel.serializable,
            readOnly: false,
            deferrable: false
        });
        this.pages = pages;
        this.name = crypto.createHash('md5').update(name).digest('hex');

        this.enque = this.enque.bind(this);
        this.tryDeque = this.tryDeque.bind(this);
        this.tryAcknowledge = this.tryAcknowledge.bind(this);
        this.#initialize = this.#initialize.bind(this);

        this.#pgBoot = new pgBootNS.PgBoot(productName + name);
    }

    #initialize = async (version) => {
        return this.#pgBoot.checkVersion(this.#writerPG, version, async (transaction, dbVersion) => {
            const ExistingQuePagesFunctionName = "Pages-" + this.name;
            const QTableName = "Q-" + this.name;
            const CursorTableName = "C-" + this.name;
            const CursorTablePKName = CursorTableName + "-PK";
            this.#queries = queries(CursorTableName, CursorTablePKName, QTableName, ExistingQuePagesFunctionName);
            switch (dbVersion) {
                case -1: //First time install
                    for (let idx = 0; idx < this.#queries.Schema1.length; idx++) {
                        let step = this.#queries.Schema1[idx];
                        let stepParams = {
                            "pagesfunctionname": ExistingQuePagesFunctionName,
                            "totalpages": this.pages,
                            "qtablename": QTableName,
                            "cursortablename": CursorTableName,
                            "cursorprimarykeyconstraintname": CursorTablePKName,
                            "gctriggerfunctionname": "GCFunc-" + this.name,
                            "gctriggername": "GC-" + this.name,
                            "dequeuefunctionname": "DQ-" + this.name,
                            "acknowledgepayloadfunctionname": "ACK-" + this.name,
                            "trydequeuefunctionname": "TRY-DQ-" + this.name,
                            "tryacknowledgepayloadfunctionname": "TRY-ACK-" + this.name
                        };
                        await transaction.none(step, stepParams);
                    };
                    break;
                case 1: //Version 1
                    //TODO Update
                    break;
                case version://Same version 
                    let results = await transaction.func(ExistingQuePagesFunctionName);
                    this.pages = results[0][ExistingQuePagesFunctionName];
                    break;
                default:
                    console.error("Unknown schema version " + dbVersion);
                    break;
            };
        });
    }

    async enque(payloads) {
        if (Array.isArray(payloads) === false || payloads.length > 40000) throw new Error("Invalid parameter payloads, expecting array of payloads not more than 40k");

        await this.#initialize(schemaVersion);

        await this.#writerPG.tx((transaction) => {
            payloads.map((p) => transaction.none(this.#queries.Enqueue, [p]));
            return transaction.batch;
        });
        //TODO Handle QUE FULL scenario(When Multiple cursors)
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
                    return transaction.any(this.#queries.Deque, [messageAcquiredTimeout, this.#cursorId]);
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
        message = message[0];
        let payload = await this.#readerPG.one(this.#queries.FetchPayload, [message.Page, message.Serial]);
        return {
            "Id": { "P": message.Page, "S": message.Serial },
            "AckToken": message.Token,
            "Payload": payload.Payload
        }
    }

    async tryAcknowledge(token, retry = 10) {
        retry = parseInt(retry);
        if (Number.isNaN(retry)) throw new Error("Invalid retry count " + retry);

        await this.#initialize(schemaVersion);

        let message;
        while (retry > 0) {
            try {
                let mode = this.#serializeTransactionMode;
                message = await this.#writerPG.tx({ mode }, transaction => {
                    return transaction.any(this.#queries.Ack, [this.#cursorId, token]);
                });
                retry = -100;
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
        if (message.length < 1 && retry === -100) {//We were sucessfull but no results
            throw new Error("Cannot acknowledge payload, cause someone else may have processed it owning to timeout(STW). Token:" + token)
        }
        else if (message != undefined && message.length > 0) {
            return message[0].Token === token;
        }
        return false;
    }
}
//Why cant this be done with pg-cursors? Cursors are session lived object not available across connections(Even WITH HOLD).