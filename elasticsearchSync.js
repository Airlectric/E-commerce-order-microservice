// orderService/elasticsearchSync.js
const mongoose = require('mongoose');
const { Client } = require('@elastic/elasticsearch');
const Order = require('./models/orderModel');

const esClient = new Client({
  node: process.env.ELASTICSEARCH_URL,
});

const INDEX_NAME = process.env.ELASTICSEARCH_INDEX || 'microservice_orders';

// Initialize Elasticsearch index with settings and mappings
const initializeIndex = async () => {
  const exists = await esClient.indices.exists({ index: INDEX_NAME });

  if (!exists) {
    await esClient.indices.create({
      index: INDEX_NAME,
      body: {
        settings: {
          number_of_shards: 1,
          number_of_replicas: 1,
        },
        mappings: {
          properties: {
            userId: { type: 'keyword' },
            products: { 
              type: 'nested', 
              properties: { 
                name: { type: 'text' }, 
                quantity: { type: 'integer' } 
              } 
            },
            totalAmount: { type: 'double' },
            status: { type: 'keyword' },
            createdAt: { type: 'date' },
            updatedAt: { type: 'date' },
          },
        },
      },
    });
    console.log(`Index "${INDEX_NAME}" created`);
  }
};

// Sync MongoDB changes to Elasticsearch in real-time
const startChangeStream = async () => {
  const connection = mongoose.connection;

  connection.once('open', () => {
    console.log('MongoDB connected for change streams.');

    const changeStream = connection.collection('orders').watch();

    changeStream.on('change', async (change) => {
      try {
        const { operationType, documentKey, fullDocument } = change;

        if (operationType === 'insert' || operationType === 'update') {
          await esClient.index({
            index: INDEX_NAME,
            id: documentKey._id,
            body: {
              userId: fullDocument.user.id,
              products: fullDocument.products,
              totalAmount: fullDocument.totalAmount,
              status: fullDocument.status,
              createdAt: fullDocument.createdAt,
              updatedAt: fullDocument.updatedAt,
            },
          });
          console.log(`Order ${documentKey._id} indexed/updated in Elasticsearch.`);
        } else if (operationType === 'delete') {
          await esClient.delete({
            index: INDEX_NAME,
            id: documentKey._id,
          });
          console.log(`Order ${documentKey._id} deleted from Elasticsearch.`);
        }
      } catch (error) {
        console.error('Error syncing change to Elasticsearch:', error);
      }
    });

    console.log('Change stream listening for updates.');
  });
};

// Search orders in Elasticsearch
const searchOrders = async (query) => {
  try {
    const { body } = await esClient.search({
      index: INDEX_NAME,
      body: {
        query: {
          bool: {
            must: [
              { match: { status: query.status || '' } },
              { range: { totalAmount: { gte: query.minAmount || 0, lte: query.maxAmount || Number.MAX_VALUE } } },
            ],
            should: query.keywords
              ? query.keywords.map((keyword) => ({ match: { 'products.name': keyword } }))
              : [],
          },
        },
      },
    });

    return body.hits.hits.map((hit) => hit._source);
  } catch (error) {
    console.error('Error searching orders in Elasticsearch:', error);
    throw error;
  }
};

// Initialize the index and set up real-time sync
const initializeAndSync = async () => {
  await initializeIndex();
  await startChangeStream();
};

module.exports = {
  initializeAndSync,
  searchOrders,
};
