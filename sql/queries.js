const path = require('path');
const pgPromise = require('pg-promise');
const QueryFile = require('pg-promise').QueryFile;
const PreparedStatement = require('pg-promise').PreparedStatement;
const pgBootNS = require("pg-boot");
function sql(file) {
    const fullPath = path.join(__dirname, file); // generating full path;
    return new QueryFile(fullPath, { minify: true });
}
const schema0 = [
    {
        "file": sql('./v1/teraform.sql'),
        "params": []
    }
]
module.exports = (cursorTableName, cursorPK, qTableName) => ({
    "Deque": pgBootNS.PgBoot.dynamicPreparedStatement("Q-Deque", `INSERT INTO $[cursorTableName:name] ("Timestamp","Serial","CursorId","Ack")
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
    RETURNING *`, { "cursorTableName": cursorTableName, "cursorPK": cursorPK, "qTableName": qTableName }),
    "Ack": pgBootNS.PgBoot.dynamicPreparedStatement('Q-Ack', `UPDATE $[cursorTableName:name] SET "Ack"=1 WHERE "CursorId"=$1 AND "Token"=$2 RETURNING *`, { "cursorTableName": cursorTableName }),
    "TimeoutSnatch": pgBootNS.PgBoot.dynamicPreparedStatement('Q-TimeoutSnatch', `UPDATE $[cursorTableName:name] SET
    "Fetched"= NOW() AT TIME ZONE 'UTC',
    "Token"= (floor(random()*(10000000-0+1))+0)
    WHERE "CursorId"=$1 AND "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Fetched") > ($2 * INTERVAL '1 Second')
    RETURNING *`, { "cursorTableName": cursorTableName }),
    "ClearQ": pgBootNS.PgBoot.dynamicPreparedStatement('Q-ClearQ', `DELETE FROM $[qTableName:name] WHERE "Timestamp" < (SELECT "Timestamp" FROM $[cursorTableName:name] ORDER BY "Timestamp" LIMIT 1 )`, { "cursorTableName": cursorTableName, "qTableName": qTableName }),
    "FetchPayload": pgBootNS.PgBoot.dynamicPreparedStatement('Q-FetchPayload', `SELECT "Payload" FROM $[qTableName:name] WHERE "Timestamp"=$1 AND "Serial"=$2`, { "qTableName": qTableName }),
    "Schema0": schema0,
})