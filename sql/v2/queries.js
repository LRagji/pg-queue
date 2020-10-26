const path = require('path');
const QueryFile = require('pg-promise').QueryFile;
const pgBootNS = require("pg-boot");
function sql(file) {
    const fullPath = path.join(__dirname, file); // generating full path;
    return new QueryFile(fullPath, { minify: true });
}
const schema1 = [//This needs to be a constant as multiple instances of the same object will create multiple handles for the same file so this is singleton
    sql('./teraform.sql')
]
module.exports = (cursorTableName, qTableName, totalPagesFunctionName) => ({
    "Enqueue": pgBootNS.PgBoot.dynamicPreparedStatement('Q-Enqueue', `INSERT INTO $[qTableName:name] ("Payload","Page")
    SELECT $1,(SELECT CASE WHEN "WriterShouldBe"="GC" THEN -1 ELSE "WriterShouldBe" END AS "Writer"
    FROM (
    SELECT COALESCE(MOD(MAX("Page")+1,$[totalpagesFunctionName:name]()),1) AS "WriterShouldBe",
    COALESCE(MIN("Page"),$[totalpagesFunctionName:name]()-1) AS "GC"
    FROM $[cursorTableName:name]
    )AS "T")`, { "cursorTableName": cursorTableName, "qTableName": qTableName, "totalpagesFunctionName": totalPagesFunctionName }),
    "Schema1": schema1
})