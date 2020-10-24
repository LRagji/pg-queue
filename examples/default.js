//This example demonstrates multiple publishers with multiple consumers with sequential reads.

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
const Q = new QType(Qname, pgWriter, 3, { "name": "Secondary", "messagesPerBatch": 3 });
const Q2 = new QType("Output", pgWriter);
let publisherHandle;

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
        publisherHandle = setTimeout(publisher, 1000);
    }).catch((err) => {
        console.error(err);
    });
}

// //Q.deleteSubscriber().then(console.log).catch(console.error).then(()=>{

// publisher();
// let reader = async () => {
//     let moreDataAvailable = false;
//     do {
//         const payload = await Q.tryDeque(10);
//         if (payload == null) {
//             moreDataAvailable = false;
//             console.log("No more data");
//         }
//         else {
//             let result = await Q.tryAcknowledge(payload.map(e => e.AckToken));
//             moreDataAvailable = result.retry === false && result.pending === null;
//             console.log(`Acked:${moreDataAvailable} Payload:${payload.length}`);
//         }
//     }
//     while (moreDataAvailable)
// };
// reader()
//     .then(console.log)
//     .catch(console.error)
// //});

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const workers = [];
if (isMainThread) {
    let numberOfSubscribers = 10;
    const processingPerSub = 10;
    while (numberOfSubscribers > 0) {
        workers.push(new Promise((resolve, reject) => {
            const worker = new Worker(__filename, { workerData: [processingPerSub, ("Sub-" + numberOfSubscribers), 100] });
            worker.on('message', resolve);
            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0)
                    reject(new Error(`Worker stopped with exit code ${code}`));
            });
        }));
        numberOfSubscribers--;
    }
    publisherHandle = setTimeout(publisher, 1000);
    Promise.all(workers).then((results) => {
        clearTimeout(publisherHandle);
        console.log(`${workers.length} Subscribers completed, Publisher Stopped.`);
        console.timeEnd("Application");
        pgp.end();
        console.table(results);
    }).catch(console.error);
    console.log(`1 Publisher(Main Thread) and ${workers.length} Subscribers active.`);
    console.time("Application");
}
else {
    const maxNumberofMessagesToFetch = workerData[0];
    const ThreadName = workerData[1];
    const sleepTime = workerData[2];
    const sleep = () => new Promise((a, r) => setTimeout(a, sleepTime));
    const waitForMessages = async (waitForMessageCount) => {
        let results = { "Subscriber": ThreadName, "Processed": [] }
        while (waitForMessageCount > 0) {
            //console.time("Deque-" + ThreadName);
            let payload = await Q.tryDeque();
            //console.timeEnd("Deque-" + ThreadName);
            if (payload != null) {
                console.log(`Thread:${ThreadName} Acquired ${payload.map(e => (e.Id.P + '-' + e.Id.S)).join(',')}`);
                let acked = false;
                while (acked === false) {
                    //console.time("Ack-" + ThreadName);
                    acked = await Q.tryAcknowledge(payload.map(e => e.AckToken));
                    acked = acked.retry === false && acked.pending === null;
                    //console.timeEnd("Ack-" + ThreadName);
                    await sleep();
                }
                const logMsg = `Thread:${ThreadName} Acknowledged:${payload.map(e => (e.Id.P + '-' + e.Id.S)).join(',')}`;
                console.log(logMsg);
                await Q2.tryEnque([{ "Log": logMsg }]);
                results.Processed.push(payload);
                waitForMessageCount--;
            }
            else {
                await sleep();
            }
        }
        return results;
    };
    waitForMessages(maxNumberofMessagesToFetch).then((r) => parentPort.postMessage(r));
}
