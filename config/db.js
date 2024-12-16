const mongoose = require('mongoose');
require('dotenv').config();

// Order Database Connection
const connectOrderDB = async () => {
  try {
    await mongoose.connect(process.env.MONGO_URI_ORDER);
    console.log('Connected to Order Database');
  } catch (err) {
    console.error('Failed to connect to Order Database:', err.message);
    process.exit(1);
  }
};



let productCacheConnection;

const connectProductCacheDB = async () => {
  try {
    if (!productCacheConnection) {
      productCacheConnection = await mongoose.createConnection(process.env.MONGO_URI_PRODUCT_CACHE);
      console.log("Connected to Product Cache Database");
    }
    return productCacheConnection;
  } catch (err) {
    console.error("Failed to connect to Product Cache Database:", err.message);
    process.exit(1); // Exit if unable to connect
  }
};


module.exports = { connectOrderDB, connectProductCacheDB };
