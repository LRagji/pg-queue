const path = require('path');
const pgPromise = require('pg-promise');
const QueryFile = require('pg-promise').QueryFile;
const pgBootNS = require("pg-boot");
function sql(file) {
    const fullPath = path.join(__dirname, file); // generating full path;
    return new QueryFile(fullPath, { minify: true });
}
const schema1 = [//This needs to be a constant as multiple instances of the same object will create multiple handles for the same file so this is singleton
    sql('./teraform.sql')
]
module.exports = (cursorTableName, cursorPK, qTableName, totalPagesFunctionName) => ({
    "Deque": pgPromise.as.format(`
    CREATE TEMP TABLE "TruncateTriggerIsolation" ON COMMIT DROP AS
    SELECT "QID"[1] AS "Serial","QID"[2] AS "Page","CursorId","Ack","Token","Fetched"
    FROM(
    SELECT
        CASE 
            WHEN "Ack"=1 AND "Serial"=9223372036854775807 THEN (SELECT ARRAY["Serial","Page"] FROM $[qTableName:name] WHERE "Serial" > 0 ORDER BY "Serial" ASC LIMIT 1)
            WHEN "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Cursor"."Fetched") > ($1 * INTERVAL '1 Second') THEN ARRAY["Serial","Page"]
            WHEN "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Cursor"."Fetched") < ($1 * INTERVAL '1 Second') THEN ARRAY[-1,-1]
            ELSE (SELECT ARRAY["Serial","Page"] FROM $[qTableName:name] WHERE "Serial" > "Cursor"."Serial" ORDER BY "Serial" ASC LIMIT 1)
        END AS "QID",
        "Cursor"."CursorId",0 AS "Ack", (floor(random()*(10000000-0+1))+0) AS "Token", NOW() AT TIME ZONE 'UTC' AS "Fetched"
    FROM (
        SELECT "Serial","Page","Ack", "Fetched","CursorId"
        FROM $[cursorTableName:name]
        WHERE "CursorId"=$2
        UNION ALL
        SELECT 0,-1,1,NOW() AT TIME ZONE 'UTC',$2
        ORDER BY "Serial" DESC
        LIMIT 1
    ) AS "Cursor"
    ) AS "Temp"
    WHERE "QID" IS NOT NULL;
    INSERT INTO $[cursorTableName:name] ("Serial","Page","CursorId","Ack","Token","Fetched")
    SELECT "Serial","Page","CursorId","Ack","Token","Fetched" FROM "TruncateTriggerIsolation"
    ON CONFLICT ON CONSTRAINT $[cursorPK:name]
    DO UPDATE 
    SET 
    "Serial"=Excluded."Serial",
    "Page"=Excluded."Page",
    "Ack"=Excluded."Ack",
    "Fetched"=Excluded."Fetched",
    "Token"= Excluded."Token"
    WHERE Excluded."Serial" != -1
    RETURNING *;`, { "cursorTableName": cursorTableName, "cursorPK": cursorPK, "qTableName": qTableName }),
    "Ack": pgBootNS.PgBoot.dynamicPreparedStatement('Q-Ack', `UPDATE $[cursorTableName:name] SET "Ack"=1 WHERE "CursorId"=$1 AND "Token"=$2 RETURNING *`, { "cursorTableName": cursorTableName }),
    "FetchPayload": pgBootNS.PgBoot.dynamicPreparedStatement('Q-FetchPayload', `SELECT "Payload" FROM $[qTableName:name] WHERE "Page"=$1 AND "Serial"=$2`, { "qTableName": qTableName }),
    "Enqueue": pgBootNS.PgBoot.dynamicPreparedStatement('Q-Enqueue', `INSERT INTO $[qTableName:name] ("Payload","Page")
    SELECT $1,(SELECT CASE WHEN "WriterShouldBe"="GC" THEN -1 ELSE "WriterShouldBe" END AS "Writer"
    FROM (
    SELECT COALESCE(MOD(MAX("Page")+1,$[totalpagesFunctionName:name]()),1) AS "WriterShouldBe",
    COALESCE(MIN("Page"),$[totalpagesFunctionName:name]()-1) AS "GC"
    FROM $[cursorTableName:name]
    )AS "T")`, { "cursorTableName": cursorTableName, "qTableName": qTableName, "totalpagesFunctionName": totalPagesFunctionName }),
    "Schema1": schema1
})