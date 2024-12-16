// order microservice consumeProductEvents.js
const { consumeMessage } = require('../config/rabbitmq');
const { connectProductCacheDB } = require('../config/db');

let ProductCache;

const connectDB = async () => {
  if (!ProductCache) {
    const connection = await connectProductCacheDB();
    ProductCache = require('../models/productCacheModel')(connection);
  }
};

const consumeProductEvents = async () => {
  try {
    await connectDB();

    // Consume from 'product_events' queue
    await consumeMessage('product_events', async (event) => {
      console.log('Received product event:', event);

      switch (event.type) {
        case 'product_created':
          await ProductCache.create(event.data);
          break;
        case 'product_updated':
          await ProductCache.findByIdAndUpdate(event.data._id, event.data, { upsert: true });
          break;
        case 'product_deleted':
          await ProductCache.findByIdAndDelete(event.data._id);
          break;
        default:
          console.warn('Unknown product event type:', event.type);
      }
    });
  } catch (err) {
    console.error('Failed to consume product events:', err.message);
  }
};

module.exports = { consumeProductEvents };
