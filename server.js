// orders microservice server.js
const express = require("express");
const { connectRabbitMQ } = require("./config/rabbitmq");
const orderRouter = require("./routes/orderRouter");
const { connectOrderDB } = require("./config/db");
const { consumeProductEvents } = require("./events/consumeProductEvents");

require("dotenv").config();

const app = express();
app.use(express.json());



(async () => {
  try {

    // Connect to the main Order DB
    connectOrderDB();

    await connectRabbitMQ();
    await consumeProductEvents();

    // Start the server
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => {
      console.log(`Order service running on port ${PORT}`);
    });
  } catch (err) {
    console.error("Failed to start server:", err.message);
    process.exit(1);
  }
})();

// Routes
app.use("/orders", orderRouter);
