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
const publisherQ = new QType(Qname, readConfigParams, writeConfigParams);
let publisherHandle;

function publisher() {
    let payloads = [];
    let ctr = 100;
    while (ctr > 0) {
        payloads.push({ "Payload": { "Id": ctr, "Time": Date.now() } });
        ctr--
    };
    //console.time("Publishing");
    publisherQ.enque(payloads).then((result) => {
        //console.timeEnd("Publishing");
        publisherHandle = setTimeout(publisher, 1000);
    }).catch((err) => console.error(err));
}

// publisherQ.tryDeque().then(console.log).catch(console.error);

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const workers = [];
if (isMainThread) {
    let numberOfSubscribers = 10;
    const processingPerSub = 10;
    while (numberOfSubscribers > 0) {
        workers.push(new Promise((resolve, reject) => {
            const worker = new Worker(__filename, { workerData: [processingPerSub, ("Sub-" + numberOfSubscribers)] });
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
    Promise.all(workers).then(console.log).catch(console.error);
    console.log("Press CTRL+C key to stop publisher.");
}
else {
    const maxNumberofMessagesToFetch = workerData[0];
    const ThreadName = workerData[1];
    const subThreadSpecificQ = new QType(Qname, readConfigParams, writeConfigParams);
    const waitForMessages = async (waitForMessageCount) => {
        let results = { "Subscriber": ThreadName, "Processed": [] }
        const sleep = (t) => new Promise((a, r) => setTimeout(a, t));
        while (waitForMessageCount > 0) {
            //console.time("Deque-" + ThreadName);
            let payload = await subThreadSpecificQ.tryDeque();
            //console.timeEnd("Deque-" + ThreadName);
            if (payload != null) {
                console.log(`Thread:${ThreadName} Acquired`);
                let acked = false;
                while (acked === false) {
                    //console.time("Ack-" + ThreadName);
                    acked = await subThreadSpecificQ.tryAcknowledge();
                    //console.timeEnd("Ack-" + ThreadName);
                    await sleep(1000);
                }
                console.log(`Thread:${ThreadName} Released`);
                results.Processed.push(payload);
                waitForMessageCount--;
            }
            else {
                await sleep(1000);
            }
        }
        return results;
    };
    waitForMessages(maxNumberofMessagesToFetch).then((r) => parentPort.postMessage(r));
}
