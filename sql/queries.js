const path = require('path');
const pgPromise = require('pg-promise');
const QueryFile = require('pg-promise').QueryFile;
const PreparedStatement = require('pg-promise').PreparedStatement;
function sql(file) {
    const fullPath = path.join(__dirname, file); // generating full path;
    return new QueryFile(fullPath, { minify: true });
}
module.exports = (cursorTableName, cursorPK, qTableName, schema, queryVersionFunctionname) => ({
    "TransactionLock": new PreparedStatement({ name: 'TransactionLock', text: `SELECT pg_try_advisory_xact_lock(hashtext($1)) as "Locked";` }),
    "Deque": new PreparedStatement({
        name: 'Deque', text: pgPromise.as.format(`INSERT INTO $[cursorTableName:name] ("Timestamp","Serial","CursorId","Ack")
    SELECT "Q"."Timestamp","Q"."Serial",$1,0
    FROM $[qTableName:name] AS "Q" 
    JOIN (
        SELECT "Timestamp","Serial"
        FROM (
            SELECT "Timestamp","Serial","CursorId","Ack" 
            FROM $[cursorTableName:name]
            WHERE "CursorId"=$1
            UNION ALL
            SELECT to_timestamp(0) AT TIME ZONE 'UTC',-1,$1,1
            ORDER BY "Timestamp" DESC
            LIMIT 1
        ) as "T"
        WHERE "Ack"=1
    ) AS "C" ON TRUE
    WHERE "Q"."Timestamp" > "C"."Timestamp" OR
    "Q"."Timestamp" = "C"."Timestamp" AND "Q"."Serial" > "C"."Serial"
    ORDER BY "Q"."Timestamp","Q"."Serial" 
    LIMIT 1
    ON CONFLICT ON CONSTRAINT $[cursorPK:name]
    DO UPDATE 
    SET 
    "Timestamp"=Excluded."Timestamp",
    "Serial"=Excluded."Serial",
    "Ack"=Excluded."Ack",
    "Fetched"= NOW() AT TIME ZONE 'UTC',
    "Token"= (floor(random()*(10000000-0+1))+0)
    RETURNING *`, { "cursorTableName": cursorTableName, "cursorPK": cursorPK, "qTableName": qTableName })
    }),
    "Ack": new PreparedStatement({ name: 'Ack', text: pgPromise.as.format(`UPDATE $[cursorTableName:name] SET "Ack"=1 WHERE "CursorId"=$1 AND "Token"=$2 RETURNING *`, { "cursorTableName": cursorTableName }) }),
    "TimeoutSnatch": new PreparedStatement({
        name: 'TimeoutSnatch', text: pgPromise.as.format(`UPDATE $[cursorTableName:name] SET
    "Fetched"= NOW() AT TIME ZONE 'UTC',
    "Token"= (floor(random()*(10000000-0+1))+0)
    WHERE "CursorId"=$1 AND "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Fetched") > ($2 * INTERVAL '1 Second')
    RETURNING *`, { "cursorTableName": cursorTableName })
    }),
    "ClearQ": new PreparedStatement({ name: 'ClearQ', text: pgPromise.as.format(`DELETE FROM $[qTableName:name] WHERE "Timestamp" < (SELECT "Timestamp" FROM $[cursorTableName:name] ORDER BY "Timestamp" LIMIT 1 )`, { "cursorTableName": cursorTableName, "qTableName": qTableName }) }),
    "FetchPayload": new PreparedStatement({ name: 'FetchPayload', text: pgPromise.as.format(`SELECT "Payload" FROM $[qTableName:name] WHERE "Timestamp"=$1 AND "Serial"=$2`, { "qTableName": qTableName }) }),
    "VersionFunctionExists": new PreparedStatement({
        name: 'VersionFunctionExists', text: pgPromise.as.format(`SELECT EXISTS(
        SELECT 1 
        FROM pg_catalog.pg_proc p
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace 
        WHERE proname = $[QVF] AND n.nspname::TEXT=$[schema])`, { "schema": schema, "QVF": queryVersionFunctionname })
    }),
    "CheckSchemaVersion": new PreparedStatement({ name: 'CheckSchemaVersion', text: pgPromise.as.format(`SELECT $[QVF:name]() AS "QueueVersion";`, { "QVF": queryVersionFunctionname }) }),
    "Schema0.0.1": [
        {
            "file": sql('./v1/teraform.sql'),
            "params": []
        }
    ],
})