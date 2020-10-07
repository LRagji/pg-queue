const queries = require('./sql/queries');
const crypto = require('crypto');
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
    #CursorTablePKName;
    #VersionFuncName;
    #qGCCounter = 0;
    #qCleanThreshhold = 100;
    #queries;
    #serializeTransactionMode;

    constructor(name, readerPG, writerPG, cleanQAfter = 10) {
        this.#readerPG = readerPG;
        this.#writerPG = writerPG;
        this.#cursorId = 1;
        this.#serializeTransactionMode = new this.#writerPG.$config.pgp.txMode.TransactionMode({
            tiLevel: this.#writerPG.$config.pgp.txMode.isolationLevel.serializable,
            readOnly: false,
            deferrable: false
        });
        this.#qCleanThreshhold = cleanQAfter;
        this.name = crypto.createHash('md5').update(name).digest('hex');
        this.#QTableName = "Q-" + this.name;
        this.#CursorTableName = "C-" + this.name;
        this.#CursorTablePKName = this.#CursorTableName + "-PK";
        this.#VersionFuncName = "QVF-" + this.name;
        this.#queries = queries(this.#CursorTableName, this.#CursorTablePKName, this.#QTableName, (this.#readerPG.$config.options.schema || 'public'), this.#VersionFuncName);
        this.enque = this.enque.bind(this);
        this.tryDeque = this.tryDeque.bind(this);
        this.tryAcknowledge = this.tryAcknowledge.bind(this);
        this.#initialize = this.#initialize.bind(this);
    }

    #initialize = async (version) => {
        if (this.#schemaInitialized === true) return;

        await this.#writerPG.tx(async transaction => {
            await transaction.one(this.#queries.TransactionLock, [("Q" + version)]);
            let someVersionExists = await this.#readerPG.one(this.#queries.VersionFunctionExists);
            if (someVersionExists.exists === true) {
                const existingVersion = await this.#readerPG.one(this.#queries.CheckSchemaVersion);
                if (existingVersion.QueueVersion === version) {
                    this.#schemaInitialized = true;
                    return;
                }
            }

            for (let idx = 0; idx < this.#queries["Schema0.0.1"].length; idx++) {
                let step = this.#queries["Schema0.0.1"][idx];
                step.params.push(this.#QTableName);
                step.params.push(this.#CursorTableName);
                step.params.push(this.#CursorTablePKName);
                step.params.push(version);
                step.params.push(this.#VersionFuncName);
                await transaction.none(step.file, step.params);
            };
        });
        this.#schemaInitialized = true;
    }

    async enque(payloads) {
        if (Array.isArray(payloads) === false) throw new Error("Invalid parameter, expecting array of payloads");

        await this.#initialize(schemaVersion);
        payloads = payloads.map(e => ({ "Payload": e }));
        const qTable = new this.#writerPG.$config.pgp.helpers.TableName({ "table": this.#QTableName });
        const columnDef = new this.#writerPG.$config.pgp.helpers.ColumnSet(["Payload:json"], { table: qTable });
        const query = this.#writerPG.$config.pgp.helpers.insert(payloads, columnDef);
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
}
//Why cant this be done with pg-cursors? Cursors are session lived object not available across connections(Even WITH HOLD).