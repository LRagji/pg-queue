//This example demonstrates how data can be enqued by the application, and later processed inside PG itself using provided stored procedure.
//This can be an efficient way of doing ETL or stream processing if your final data resting  place is PG itself.

const QType = require('../pg-que');
const initOptions = {
    // query(e) {
    //     console.log(e.query);
    // },
    //"schema": "Q"
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
const Q = new QType(Qname, pgWriter, 3, { "name": "Subscriber-1", "messagesPerBatch": 3 });
let publisherHandle, processorHandle;

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
            publisherHandle = new Timeout(publisher, 500);
        }
    }).catch((err) => {
        console.error(err);
    });

}

let Subcriber = async () => {
    let result;
    do {
        result = await Q.tryDeque(10);
        if (result == null) {
            console.log("No more data");
        }
        else {
            let acked = await Q.tryAcknowledge(result.map(e => e.AckToken));
            if (acked == undefined) {
                console.log(`Acked` + result);
            }
            else {
                throw new Error('Failed to ack' + result);
            }
        }
    }
    while (result != undefined)
};

publisher();
console.log("Publisher Active");

// Subcriber()
//     .then(console.log)
//     .catch(console.error)
// console.log("Processor Active");

console.log("Press Ctrl+C to stop processing");