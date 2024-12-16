// order microservice rabbitmq.js
const amqp = require('amqplib');

let connection, channel;

const connectRabbitMQ = async () => {
  try {
    if (!connection || !channel) {
      connection = await amqp.connect(process.env.RABBITMQ_URL);
      channel = await connection.createChannel();
      
      console.log('RabbitMQ connected in Order Microservice');

      // Declare all required queues
      await channel.assertQueue('order_events_for_notifications', { durable: true });
      await channel.assertQueue('product_events', { durable: true });
    }

    return { channel, connection };
  } catch (err) {
    console.error('Failed to connect to RabbitMQ:', err.message);
    throw err;
  }
};

const sendMessage = async (queue, message) => {
  try {
    const { channel } = await connectRabbitMQ();

    // Send the message to the specified queue
    channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), { persistent: true });

    console.log(`Message sent to ${queue}:`, message);
  } catch (err) {
    console.error(`Failed to send message to ${queue}:`, err.message);
  }
};

const consumeMessage = async (queue, callback) => {
  try {
    const { channel } = await connectRabbitMQ();

    // Consume messages from the specified queue
    channel.consume(queue, async (msg) => {
      if (msg !== null) {
        try {
          const messageContent = JSON.parse(msg.content.toString());
          console.log(`Received message from ${queue}:`, messageContent);
          await callback(messageContent); // Call the provided callback with the message content
          channel.ack(msg); // Acknowledge successful processing
        } catch (err) {
          console.error(`Error processing message from ${queue}:`, err.message);
          channel.nack(msg); // Negative acknowledgment (requeue the message)
        }
      }
    });

    console.log(`Listening for messages on queue: ${queue}`);
  } catch (err) {
    console.error(`Failed to consume messages from ${queue}:`, err.message);
  }
};

module.exports = { connectRabbitMQ, sendMessage, consumeMessage };
