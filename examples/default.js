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

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const workers = [];
if (isMainThread) {
    let numberOfSubscribers = 10;
    const processingPerSub = 10;
    while (numberOfSubscribers > 0) {
        workers.push(new Promise((resolve, reject) => {
            const worker = new Worker(__filename, { workerData: [processingPerSub, ("Sub-" + numberOfSubscribers), 100, numberOfSubscribers] });
            worker.on('message', resolve);
            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0)
                    reject(new Error(`Worker stopped with exit code ${code}`));
            });
        }));
        numberOfSubscribers--;
    }
    publisherHandle = new Timeout(publisher, 100);
    Promise.all(workers).then((results) => {
        publisherHandle.clear();
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
    const sleep = (t = (sleepTime)) => new Promise((a, r) => setTimeout(a, t));
    const waitForMessages = async (waitForMessageCount) => {
        let results = { "Subscriber": ThreadName, "Processed": [] }
        while (waitForMessageCount > 0) {
            let watchDogSleepCounter = 0;
            //console.time("Deque-" + ThreadName);
            //console.log(`Thread:${ThreadName} 1`);
            let payload = await Q.tryDeque();
            //console.log(`Thread:${ThreadName} 2`);
            //console.timeEnd("Deque-" + ThreadName);
            if (payload != null) {
                console.log(`Thread:${ThreadName} Acquired ${payload.map(e => (e.Id.P + '-' + e.Id.S)).join(',')}`);
                let tokens = payload.map(e => e.AckToken);
                let watchDogLoopCounter = 0;
                while (tokens != undefined) {
                    //console.time("Ack-" + ThreadName);
                    //console.log(`Thread:${ThreadName} 3`);
                    tokens = await Q.tryAcknowledge(tokens);
                    //console.log(`Thread:${ThreadName} Ack :${JSON.stringify(acked)}`);
                    //console.log(`Thread:${ThreadName} 4`);
                    //console.timeEnd("Ack-" + ThreadName);
                    if (watchDogLoopCounter > 2) console.log(`Thread:${ThreadName} Watchdog Ctr:${watchDogLoopCounter}`);
                    await sleep();
                    watchDogLoopCounter++;
                }
                const logMsg = `Thread:${ThreadName} Acknowledged:${payload.map(e => (e.Id.P + '-' + e.Id.S)).join(',')}`;
                console.log(logMsg);
                //console.log(`Thread:${ThreadName} 5`);
                await Q2.tryEnque([{ "Log": logMsg }]);
                //console.log(`Thread:${ThreadName} 6`);
                results.Processed.push(payload);
                waitForMessageCount--;
            }
            else {
                if (watchDogSleepCounter > 2) console.log(`Thread:${ThreadName} Watchdog Ctr:${watchDogSleepCounter}`);
                await sleep();
            }
            watchDogSleepCounter++;
        }
        return results;
    };
    waitForMessages(maxNumberofMessagesToFetch).then((r) => parentPort.postMessage(r));
}
