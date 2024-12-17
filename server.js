// orders microservice server.js
const express = require("express");
const { connectRabbitMQ } = require("./config/rabbitmq");
const orderRouter = require("./routes/orderRouter");
const { connectOrderDB } = require("./config/db");
const { consumeProductEvents } = require("./events/consumeProductEvents");
const syncProductCache = require("./services/productCacheSyncService");
const { initializeAndSync } = require('./elasticsearchSync');

require("dotenv").config();

const app = express();
app.use(express.json());



(async () => {
  try {

    // Connect to the main Order DB
    connectOrderDB();
	
	// Start ProductCache Sync Service
    await syncProductCache();

    await connectRabbitMQ();
    // await consumeProductEvents();
	

	initializeAndSync().catch((error) => {
	  console.error('Failed to initialize and sync:', error);
	});


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
