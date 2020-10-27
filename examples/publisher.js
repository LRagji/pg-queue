//This example demonstrates multiple publishers.

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
let publisherHandle;

function Timeout(fn, interval) {
    var id = setTimeout(fn, interval);
    this.cleared = false;
    this.clear = function () {
        this.cleared = true;
        clearTimeout(id);
    };
}

function publisher() {
    let payloads = [];
    let ctr = 1000;
    while (ctr > 0) {
        payloads.push({ "Counter": ctr });
        ctr--
    };
    //console.time("Publishing");
    Q.tryEnque(payloads).then((result) => {
        //console.timeEnd("Publishing");
        if (publisherHandle == undefined || (!publisherHandle.cleared)) {
            publisherHandle = new Timeout(publisher, 1000);
        }
    }).catch((err) => {
        console.error(err);
    });

}

publisher();
publisher();
publisher();
console.log("Publisher Active");

console.log("Press Ctrl+c to stop publisher");