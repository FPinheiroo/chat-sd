const zmq = require("zeromq");

async function main() {
  const proxy = new zmq.Proxy(new zmq.XSubscriber(), new zmq.XPublisher());
  await proxy.frontEnd.bind("tcp://*:5557");
  await proxy.backEnd.bind("tcp://*:5558");
  console.log("proxy pub/sub: XSUB na 5557, XPUB na 5558");
  await proxy.run();
}

main();
