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
module.exports = () => ({
    "Schema1": schema1
})