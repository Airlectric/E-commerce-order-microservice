const mongoose = require('mongoose');
const { Client } = require('@elastic/elasticsearch');
const ProductCache = require('../models/productCacheModel'); // Updated schema

const esClient = new Client({
  node: process.env.ELASTICSEARCH_URI,
});

const INDEX_NAME = process.env.ELASTICSEARCH_PRODUCT_INDEX || 'order_microservice_products';

// Initialize Elasticsearch index with settings and mappings
const initializeProductIndex = async () => {
  try {
    const exists = await esClient.indices.exists({ index: INDEX_NAME });

    if (!exists.body) {
      await esClient.indices.create({
        index: INDEX_NAME,
        body: {
          settings: {
            number_of_shards: 1,
            number_of_replicas: 1,
          },
          mappings: {
            properties: {
              title: { type: 'text' },
              description: { type: 'text' },
              category_id: { type: 'keyword' },
              category: { type: 'text' },
              price: { type: 'double' },
              quantity: { type: 'integer' },
              image: { type: 'keyword' },
              seller: {
                properties: {
                  id: { type: 'keyword' },
                },
              },
              updatedAt: { type: 'date' },
            },
          },
        },
      });
      console.log(`Index "${INDEX_NAME}" created for products`);
    }
  } catch (error) {
    console.error('Error initializing Elasticsearch index:', error);
  }
};

// Bootstrap existing MongoDB data into Elasticsearch
const bootstrapElasticsearchIndex = async () => {
  try {
    const products = await ProductCache.find({});
    console.log(`Found ${products.length} products in MongoDB.`);

    for (const product of products) {
      const productId = product._id.toString();

      // Check if the product already exists in Elasticsearch
      const exists = await esClient.exists({
        index: INDEX_NAME,
        id: productId,
      });

      if (exists.body) {
        console.log(`Product ID: ${productId} already exists in Elasticsearch. Skipping...`);
        continue; // Skip to the next product
      }

      // If not exists, index the product
      await esClient.index({
        index: INDEX_NAME,
        id: productId,
        body: {
          title: product.title,
          description: product.description,
          category_id: product.category_id,
          category: product.category,
          price: product.price,
          quantity: product.quantity,
          image: product.image,
          seller: product.seller,
          updatedAt: product.updatedAt,
        },
      });

      console.log(`Indexed product ID: ${productId} to Elasticsearch.`);
    }
    console.log('Bootstrap indexing completed.');
  } catch (error) {
    console.error('Error bootstrapping Elasticsearch index:', error);
  }
};


// Sync MongoDB changes to Elasticsearch in real-time
const startProductChangeStream = async () => {
  const connection = mongoose.connection;

  connection.once('open', () => {
    console.log('MongoDB connected for product change streams.');

    const changeStream = connection.collection('productcaches').watch();

    changeStream.on('change', async (change) => {
      try {
        const { operationType, documentKey, fullDocument } = change;

        if (operationType === 'insert' || operationType === 'update') {
          await esClient.index({
            index: INDEX_NAME,
            id: documentKey._id,
            body: {
              title: fullDocument.title,
              description: fullDocument.description,
              category_id: fullDocument.category_id,
              category: fullDocument.category,
              price: fullDocument.price,
              quantity: fullDocument.quantity,
              image: fullDocument.image,
              seller: fullDocument.seller,
              updatedAt: fullDocument.updatedAt,
            },
          });
          console.log(`Product ${documentKey._id} indexed/updated in Elasticsearch.`);
        } else if (operationType === 'delete') {
          await esClient.delete({
            index: INDEX_NAME,
            id: documentKey._id,
          });
          console.log(`Product ${documentKey._id} deleted from Elasticsearch.`);
        }
      } catch (error) {
        console.error('Error syncing product change to Elasticsearch:', error);
      }
    });

    console.log('Change stream listening for product updates.');
  });
};

// Search products in Elasticsearch
const searchProducts = async (query) => {
  try {
    const filters = [];

    // Use match queries for text fields
    if (query.title) {
      filters.push({
        match: { title: { query: query.title, fuzziness: 'AUTO' } },
      });
    }
    if (query.description) {
      filters.push({
        match: { description: { query: query.description, fuzziness: 'AUTO' } },
      });
    }
    if (query.category) {
      filters.push({
        match: { category: { query: query.category, fuzziness: 'AUTO' } },
      });
    }

    // Range queries for price
    if (query.price && (query.price.min || query.price.max)) {
      filters.push({
        range: {
          price: {
            ...(query.price.min ? { gte: query.price.min } : {}),
            ...(query.price.max ? { lte: query.price.max } : {}),
          },
        },
      });
    }

    // Build the query
    const queryBody = filters.length > 0
      ? { query: { bool: { must: filters } } }
      : { query: { match_all: {} } };

    // Execute search
    const response = await esClient.search({
      index: INDEX_NAME,
      body: queryBody,
    });

    // Debug the entire response for better visibility
    console.log('Elasticsearch Response:', JSON.stringify(response, null, 2));

    // Safely access and map the hits
    if (response.hits && response.hits.hits) {
      return response.hits.hits.map((hit) => hit._source);
    } else {
      console.warn('No hits found in response:', response);
      return [];
    }
  } catch (error) {
    console.error('Error in searchProducts:', error.meta?.body?.error || error);

    // Log additional error details for debugging
    if (error.response) {
      console.error('Error response from Elasticsearch:', JSON.stringify(error.response.body, null, 2));
    }

    throw error; // Rethrow the error after logging it
  }
};



// Initialize the index and set up real-time sync
const initializeAndSyncProducts = async () => {
  await initializeProductIndex(); // Ensure index exists
  await bootstrapElasticsearchIndex(); // Index existing data
  await startProductChangeStream(); // Start watching for real-time changes
};

module.exports = {
  initializeAndSyncProducts,
  searchProducts,
};
