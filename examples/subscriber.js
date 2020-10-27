//This example demonstrates single subscriber with sequential reads for 10 items in bulk

const QType = require('../pg-que');
const initOptions = {
    // query(e) {
    //     console.log(e.query);
    // },
    "schema": "Q"
};
const pgp = require('pg-promise')(initOptions);
const defaultConectionString = "postgres://postgres:@localhost:5432/QUEUE";
const writeConfigParams = {
    connectionString: defaultConectionString,
    application_name: "Example1-Queue-Writer",
    max: 2 //2 Writer
};

const Qname = "Laukik";
// Awaiting Bug https://github.com/brianc/node-postgres/issues/2363 to move this inside library
pgp.pg.types.setTypeParser(20, BigInt); // This is for serialization bug of BigInts as strings.
pgp.pg.types.setTypeParser(1114, str => str); // UTC Timestamp Formatting Bug, 1114 is OID for timestamp in Postgres.
let pgWriter = pgp(writeConfigParams);
const Q = new QType(Qname, pgWriter);

let Subcriber = async () => {
    let result;
    do {
        result = await Q.tryDeque(10);
        if (result == null) {
            console.log("No more data");
        }
        else {
            result = await Q.tryAcknowledge(result.map(e => e.AckToken));
            console.log(`Acked`);
        }
    }
    while (result != undefined)
};


Subcriber()
    .then(console.log)
    .catch(console.error)
console.log("Subcriber Active");

console.log("Press Ctrl+c to stop publisher");