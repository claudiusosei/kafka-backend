const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

const kafka = require("kafka-node");
var event = require("events");

const Consumer = kafka.Consumer;
const Producer = kafka.Producer;

const router = require("./src/routes/index.router");
// const produce = require("./produce");
// const consume = require("./consume");

const app = express();

const corsOpts = {
	origin: "*",
	methods: ["GET", "POST", "PUT", "PETCH"],
	allowedHeaders: ["Content-Type"],
  };
  app.use(cors(corsOpts));
app.use(function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});


app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// call the `produce` function and log an error if it occurs
produce().catch((err) => {
  console.error("error in producer: ", err);
});

// start the consumer, and log any errors
consume().catch((err) => {
  console.error("error in consumer: ", err);
});

const consumer = new Consumer(new kafka.KafkaClient(), [{ topic: "test" }], {
  groupId: "your-consumer-group-id",
});

consumer.on("message", (message) => {
  // Process the received message here
  console.log("Received message:", message);
});

app.get("/", (req, res) => {
  // #swagger.ignore = true
  res.json({
    status: "NO DATA",
  });
});
const port = process.env.PORT ?? 4000;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use("/api/v1", router);

app.post("/send", function (req, res) {
  const producer = new Producer(new kafka.KafkaClient());

  var sentMessage = JSON.stringify(req.body.message);
  payloads = [{ topic: req.body.topic, messages: sentMessage, partition: 0 }];
  producer.send(payloads, function (err, data) {
    res.json(data);
  });
  //console.log(req.body.topic);
});

global.kontrol = false;

app.post("/changetopic", function (req, res) {
  //var topic = JSON.stringify(req.body.topic);
  // payloads = [
  //     { topic: req.body.topic}
  // ];

  //consumer.payloads[0].topic
  consumer.addTopics([req.body.topic], function (err, added) {
    console.log(added + " eklendi.");
  });

  //console.log(req.body.topic);
  //consumer.pause();
  //consumer.pause();
  //consumer.payloads[0].topic=req.body.topic;
  //global.dataarray = [];
  consumer.resume();
  console.log(consumer.payloads[0]);
  global.kontrol = true;
  res.json(global.kontrol);

  //console.log("burada");
});

app.post("/fetchTopicViseData", async function (req, res, next) {
  const consumer = new Consumer(new kafka.KafkaClient(), [{ topic: req.body?.topic }], {
    groupId: "your-consumer-group-id",
  });

  consumer.on("message", (message) => {
    // Process the received message here
    console.log("req.body?.topic:", message);
	res.json(message);

  });
//   res.json(consumer);

  
});

const eventEmitter2 = new event.EventEmitter();
eventEmitter2.setMaxListeners(150);

app.post("/createtopic", function (req, res) {
  var client = new kafka.KafkaClient();

  var topicsToCreate = [
    {
      topic: req.body.createtopic,
      partitions: 1,
      replicationFactor: 1,
    },
  ];
  client.createTopics(topicsToCreate, (error, result) => {
	res.json(result);
  });
});

const eventEmitter3 = new event.EventEmitter();
eventEmitter3.setMaxListeners(150);

global.listtopic = [];
app.get("/listtopics", function (req, res) {
  const client = new kafka.KafkaClient();
  const admin = new kafka.Admin(client);
  global.res1 = res;

  global.data1;
  admin.listTopics((err, res) => {
    eleman = res[1].metadata;
    var newarray = [eleman];
    //var topiclist=[];
    var topiclist = [];
    Object.keys(newarray[0])
      .filter(function (key) {
        topiclist.push(key);
      })
      .join(" ");

    //console.log(topiclist);
    global.res1.json(topiclist);
    global.listtopic = topiclist;
    eventEmitter3.emit("trigsend");
  });
});

const http = require("http");
const socketIO = require("socket.io");
const server = http.createServer(app);
const io = socketIO(server);
const eventEmitter = new event.EventEmitter();
eventEmitter.setMaxListeners(150);

// var kafka = require('kafka-node'),
// Consumer = kafka.Consumer,
// client = new kafka.KafkaClient(),
// consumer = new Consumer(client,
// [{ topic: 'emptytopic', offset: 0}],
// {
//     groupId: 'kaanbayram',
//     autoCommit: false
// });

global.dataarray = [];

// consumer.removeTopics(['topic1', 'topic2','topic3'], function (err, removed) {
//     console.log('silimdi '+removed);
// });

consumer.on("message", function (message) {
  if (global.kontrol === true) {
    count = global.dataarray.length;
    while (count > 0) {
      global.dataarray.pop();
      count = count - 1;
    }
    global.kontrol = false;
  }

  console.log(message.value);
  dataarray.push(message.value);
  eventEmitter.emit("trigger");
});

io.on("connection", (socket) => {
  console.log("User connected");
  io.sockets.emit("getmessage", { data: dataarray });

  eventEmitter.on("trigger", () => {
    io.sockets.emit("getmessage", { data: dataarray });
  });
});

io.on("connection", (socket) => {
  console.log("user connected socket2");
  io.sockets.emit("sendlist", { data: listtopic });

  eventEmitter3.on("trigsend", () => {
    io.sockets.emit("sendlist", { data: listtopic });
  });
});
// Start the server
async function initialise() {
  try {
    app.listen(port, () => {
      console.log(`Server running on :${port}`);
    });
  } catch (error) {
    console.log(`Unable to connect to the database: ${JSON.stringify(error)}`);
  }
}

initialise();

//error handler middleware
app.use((err, req, res, next) => {
  console.log(err.stack);
  res.status(500).send({
    status: 500,
    message: err.message,
  });
});
