const kafka = require("kafka-node");
const Consumer = kafka.Consumer;
const Producer = kafka.Producer;

// User Registration API
module.exports.sendMessage = async (req, res) => {
  // #swagger.tags = ['Users']
  // #swagger.summary = 'Registration API'
  /* #swagger.requestBody['body'] = {
        in: 'body',
        description: 'Add a user',
        schema: { $ref: '#/definitions/UserObj' }
    } */
  const producer = new Producer(new kafka.KafkaClient());
  producer.on("ready", () => {
    // Produce a message
    const message = {
      topic: req?.body?.topic,
      messages: req?.body?.messages,
      partition: 0,
    };
    producer.send([message], (err, data) => {
      if (err) {
        console.error("Failed to send message:", err);
      } else {
        console.log("Message sent:", data);
      }
    });
  });

  producer.on("error", (error) => {
    console.error("Producer error:", error);
  });
  process.on("SIGINT", () => {
    consumer.close(() => {
      console.log("Kafka consumer closed.");
      process.exit();
    });
  });
  return res.status("200").send("secuus");
};

module.exports.receivedMessgaeByTopic = async (req, res) => {
  console.log("===> ", req.body);
  const consumer = new Consumer(
    new kafka.KafkaClient(),
    [{ topic: req?.body?.topic }],
    { groupId: "your-consumer-group-id" }
  );

  consumer.on('message', (message) => {
    // Process the received message here
    console.log('Received message ===> :', message);
  });
  //  const data = await consumer.on('message', (message) => {
  //     // Process the received message here
  //     console.log('Received message:', message);
  //     return message;
  //     // res.status("200").send(message);
  //   });
  consumer.on("error", (error) => {
    console.error("Consumer error:", error);
  });

  return res.status("200").send("test");
};
