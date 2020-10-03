//This example demonstrates multiple publishers with multiple consumers with sequential reads.

const QType = require('../index');
const defaultConectionString = "postgres://postgres:@localhost:5432/QUEUE";
const readConfigParams = {
    connectionString: defaultConectionString,
    application_name: "Example1-Queue-Reader",
    max: 4 //4 readers
};
const writeConfigParams = {
    connectionString: defaultConectionString,
    application_name: "Example1-Queue-Writer",
    max: 2 //2 Writer
};
const Qname = "Laukik";
const Q = new QType(Qname, readConfigParams, writeConfigParams);
let publisherHandle;

function publisher() {
    let payloads = [];
    let ctr = 100;
    while (ctr > 0) {
        payloads.push({ "Payload": { "Id": ctr, "Time": Date.now() } });
        ctr--
    };
    //console.time("Publishing");
    Q.enque(payloads).then((result) => {
        //console.timeEnd("Publishing");
        publisherHandle = setTimeout(publisher, 1000);
    }).catch((err) => console.error(err));
}


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
        Q.dispose();
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
                console.log(`Thread:${ThreadName} Acquired ${payload.Id.T}-${payload.Id.S}`);
                let acked = false;
                while (acked === false) {
                    //console.time("Ack-" + ThreadName);
                    acked = await Q.tryAcknowledge(payload.AckToken);
                    //console.timeEnd("Ack-" + ThreadName);
                    await sleep();
                }
                console.log(`Thread:${ThreadName} Acknowledged ${payload.Id.T}-${payload.Id.S}`);
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
