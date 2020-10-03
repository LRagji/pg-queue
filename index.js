const queries = require('./sql/queries');
const pg = require('pg-promise')
const schemaVersion = "0.0.1";
const serializationError = '40001';
module.exports = class PgQueue {

    name;
    #cursorId;
    #readerPG;
    #writerPG;
    #schemaInitialized = false;
    #QTableName;
    #CursorTableName;
    #qGCCounter = 0;
    #qCleanThreshhold = 100;
    #queries;
    #pgp;
    #serializeTransactionMode;

    static checkForSimilarConnection(aConfigParams, bConfigParams, aConnection, newConnectionCallback) {
        if (aConfigParams === bConfigParams) {
            return aConnection;
        }
        else {
            return newConnectionCallback(bConfigParams);
        }
    }

    constructor(name, readerPG, writerPG, schema = 'public', cleanQAfter = 10) {
        this.#cursorId = 1;
        const initOptions = {
            // query(e) {
            //     console.log(e.query);
            // },
            "schema": schema
        };
        this.#pgp = pg(initOptions);
        this.#pgp.pg.types.setTypeParser(20, BigInt); // This is for serialization bug of BigInts as strings.
        this.#pgp.pg.types.setTypeParser(1114, str => str); // UTC Timestamp Formatting Bug, 1114 is OID for timestamp in Postgres.
        this.#serializeTransactionMode = new this.#pgp.txMode.TransactionMode({
            tiLevel: this.#pgp.txMode.isolationLevel.serializable,
            readOnly: false,
            deferrable: false
        });
        this.#qCleanThreshhold = cleanQAfter;
        this.name = name;
        this.#QTableName = "Q-" + this.name;
        this.#CursorTableName = "Cursor-" + this.name;
        this.#queries = queries(this.#CursorTableName, this.#QTableName);
        this.enque = this.enque.bind(this);
        this.tryDeque = this.tryDeque.bind(this);
        this.tryAcknowledge = this.tryAcknowledge.bind(this);
        this.dispose = this.dispose.bind(this);
        this.#initialize = this.#initialize.bind(this);
        this.#readerPG = this.#pgp(readerPG);
        this.#writerPG = PgQueue.checkForSimilarConnection(readerPG, writerPG, this.#readerPG, (c) => this.#pgp(c));
    }

    #initialize = async (version) => {
        if (this.#schemaInitialized === true) return;
        let someVersionExists = await this.#readerPG.one(this.#queries.VersionFunctionExists);
        if (someVersionExists.exists === true) {
            const existingVersion = await this.#readerPG.one(this.#queries.CheckSchemaVersion);
            if (existingVersion.QueueVersion === version) {
                this.#schemaInitialized = true;
                return;
            }
        }

        await this.#writerPG.tx(async transaction => {
            let acquired = await transaction.one(this.#queries.TransactionLock, [("Q" + version)]);
            if (acquired.Locked === true) {
                for (let idx = 0; idx < this.#queries["Schema0.0.1"].length; idx++) {
                    let step = this.#queries["Schema0.0.1"][idx];
                    step.params.push(this.#QTableName);
                    step.params.push(this.#CursorTableName);
                    step.params.push(version);
                    await transaction.none(step.file, step.params);
                };
            };
            return;
        });
        this.#schemaInitialized = true;
    }

    async enque(payloads) {
        if (Array.isArray(payloads) === false) throw new Error("Invalid parameter, expecting array of payloads");

        await this.#initialize(schemaVersion);
        payloads = payloads.map(e => ({ "Payload": e }));
        const qTable = new this.#pgp.helpers.TableName({ "table": this.#QTableName });
        const columnDef = new this.#pgp.helpers.ColumnSet(["Payload:json"], { table: qTable });
        const query = this.#pgp.helpers.insert(payloads, columnDef);
        await this.#writerPG.none(query);
        this.#qGCCounter++;
        if (this.#qGCCounter >= this.#qCleanThreshhold) {
            await this.#writerPG.none(this.#queries.ClearQ);
            this.#qGCCounter = 0;
        }
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
            //Try for timed out messages
            try {
                let mode = this.#serializeTransactionMode;
                message = await this.#writerPG.tx({ mode }, transaction => {
                    return transaction.any(this.#queries.TimeoutSnatch, [this.#cursorId, messageAcquiredTimeout]);
                })
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
            if (message !== undefined && message.length > 0) {
                retry = 0;
                continue;
            }

            //Try acquiring new messages
            try {
                let mode = this.#serializeTransactionMode;
                message = await this.#writerPG.tx({ mode }, transaction => {
                    return transaction.any(this.#queries.Deque, [this.#cursorId]);
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
            retry--;
        }
        if (message.length <= 0) return undefined;
        message = message[0];
        let payload = await this.#readerPG.one(this.#queries.FetchPayload, [message.Timestamp, message.Serial]);
        return {
            "Id": { "T": message.Timestamp, "S": message.Serial },
            "AckToken": message.Token,
            "Payload": payload[0]
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
                    retry--;
                    continue;
                }
                else {
                    throw err;
                }
            }
        }
        if (message.length < 1 && retry === -100) {//We were sucessfull but no results
            throw new Error("Cannot acknowledge payload, cause someone else may have processed it owning to timeout(STW). Token:" + token)
        }
        else if (message.length > 0) {
            return message[0].Token === token;
        }
        return false;
    }

    dispose() {
        this.#pgp.end();
    }
}
//Why cant this be done with pg-cursors? Cursors are session lived object not available across connections(Even WITH HOLD).