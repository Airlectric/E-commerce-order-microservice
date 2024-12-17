const { searchOrders } = require('./elasticsearchSync');

const searchQuery = {
  status: 'completed',
  minAmount: 50,
  maxAmount: 500,
  keywords: ['laptop', 'headphones'],
};

searchOrders(searchQuery)
  .then((results) => console.log('Search Results:', results))
  .catch(console.error);
