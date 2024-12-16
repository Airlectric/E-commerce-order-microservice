
// elasticsearchSync.js (Example Syncing Script)
const Order = require('./models/orderModel');
const elasticsearch = require('elasticsearch');

const esClient = new elasticsearch.Client({
  host: process.env.ELASTICSEARCH_URL,
});

const syncOrdersToElasticsearch = async () => {
  const orders = await Order.find();

  for (const order of orders) {
    await esClient.index({
      index: 'orders',
      id: order.id,
      body: {
        userId: order.user.id,
        products: order.products,
        totalAmount: order.totalAmount,
        status: order.status,
        createdAt: order.createdAt,
        updatedAt: order.updatedAt,
      },
    });
  }

  console.log('Orders synced to Elasticsearch');
};

syncOrdersToElasticsearch().catch(console.error);
