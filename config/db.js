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

module.exports = { connectOrderDB };
