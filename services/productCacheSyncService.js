const mongoose = require('mongoose');
const ProductCache = require('../models/productCacheModel'); // ProductCache model in the orders microservice
require('dotenv').config();

let retryQueue = []; // Temporary queue for failed operations

const syncProductCache = async () => {
  try {
    // Connect to the Product microservice database
    const productDB = await mongoose.createConnection(process.env.MONGO_URI_PRODUCT);
    const ProductModel = productDB.model(
      'Product',
      new mongoose.Schema({
        title: { type: String, required: true },
        description: { type: String, required: true },
        category_id: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
        price: { type: Number, required: true },
        quantity: { type: Number, required: true },
        image: { type: String },
        imageId: { type: mongoose.Schema.Types.ObjectId, ref: 'fs.files' },
        seller: {
          id: { type: String, required: true },
          profileUrl: { type: String },
          profileImageId: { type: mongoose.Schema.Types.ObjectId, ref: 'fs.files' },
        },
      })
    );

    const CategoryModel = productDB.model(
      'Category',
      new mongoose.Schema({
        name: { type: String, required: true },
      })
    );

    console.log('Performing initial sync...');

    // Perform the initial synchronization
    await performInitialSync(ProductModel, CategoryModel);

    console.log('Listening to Product collection for changes...');

    // Start watching Change Streams on the Product collection
    const changeStream = ProductModel.watch([], { fullDocument: 'updateLookup' });

    changeStream.on('change', async (change) => {
      try {
        console.log('Change detected in Product Collection:', change);

        switch (change.operationType) {
          case 'insert':
            await handleCacheSync(async () => {
              const populatedProduct = await ProductModel.findById(change.fullDocument._id).populate('category_id');
              await ProductCache.create({
                _id: populatedProduct._id,
                ...populateProductCacheData(populatedProduct),
                updatedAt: new Date(),
              });
            });
            console.log(`Inserted product ${change.fullDocument.title} into ProductCache`);
            break;

          case 'update':
            await handleCacheSync(async () => {
              const populatedProduct = await ProductModel.findById(change.fullDocument._id).populate('category_id');
              await ProductCache.updateOne(
                { _id: populatedProduct._id },
                { $set: { ...populateProductCacheData(populatedProduct), updatedAt: new Date() } },
                { upsert: true }
              );
            });
            console.log(`Updated product ${change.fullDocument.title} in ProductCache`);
            break;

          case 'delete':
            await handleCacheSync(() =>
              ProductCache.deleteOne({ _id: change.documentKey._id })
            );
            console.log(`Deleted product with ID ${change.documentKey._id} from ProductCache`);
            break;

          default:
            console.log('Unrecognized change event:', change.operationType);
        }
      } catch (err) {
        console.error('Error processing product change stream:', err.message);
        addToRetryQueue(change); // Add the change to the retry queue
      }
    });

    changeStream.on('error', (err) => {
      console.error('Error in change stream:', err.message);
    });

    // Periodically retry failed operations
    setInterval(() => retryFailedOperations(ProductModel), 5000);
  } catch (err) {
    console.error('Error starting Product Cache Sync:', err.message);
  }
};

// Perform initial synchronization
const performInitialSync = async (ProductModel, CategoryModel) => {
  try {
    // Fetch all products from the Product model
    const allProducts = await ProductModel.find().lean();

    // Iterate through all products and ensure they're in the cache
    for (const product of allProducts) {
      const cacheEntry = await ProductCache.findById(product._id);
      const populatedProduct = await ProductModel.findById(product._id).populate('category_id');

      if (!cacheEntry) {
        // Insert if not present in the cache
        await ProductCache.create({
          _id: populatedProduct._id,
          ...populateProductCacheData(populatedProduct),
          updatedAt: new Date(),
        });
        console.log(`Inserted missing product ${product.title} into ProductCache`);
      } else if (new Date(product.updatedAt) > new Date(cacheEntry.updatedAt)) {
        // Update if outdated in the cache
        await ProductCache.updateOne(
          { _id: populatedProduct._id },
          { $set: { ...populateProductCacheData(populatedProduct), updatedAt: new Date() } }
        );
        console.log(`Updated outdated product ${product.title} in ProductCache`);
      }
    }

    console.log('Initial synchronization complete.');
  } catch (err) {
    console.error('Error during initial sync:', err.message);
  }
};

// Handle cache synchronization with error handling
const handleCacheSync = async (operation) => {
  try {
    await operation(); // Attempt the operation
  } catch (err) {
    console.error('Cache sync operation failed:', err.message);
    throw err; // Rethrow the error to handle it outside
  }
};

// Add a failed operation to the retry queue
const addToRetryQueue = (change) => {
  retryQueue.push(change);
};

// Retry failed operations in the queue
const retryFailedOperations = async (ProductModel) => {
  if (retryQueue.length === 0) return; // Exit if the queue is empty

  console.log('Retrying failed operations...');
  const failedChanges = [...retryQueue];
  retryQueue = []; // Clear the queue temporarily

  for (const change of failedChanges) {
    try {
      switch (change.operationType) {
        case 'insert':
          const populatedProductInsert = await ProductModel.findById(change.fullDocument._id).populate('category_id');
          await ProductCache.create({
            ...populateProductCacheData(populatedProductInsert),
            updatedAt: new Date(),
          });
          console.log(`Retried insert for product ${populatedProductInsert.title}`);
          break;

        case 'update':
          const populatedProductUpdate = await ProductModel.findById(change.fullDocument._id).populate('category_id');
          await ProductCache.updateOne(
            { _id: populatedProductUpdate._id },
            { $set: { ...populateProductCacheData(populatedProductUpdate), updatedAt: new Date() } },
            { upsert: true }
          );
          console.log(`Retried update for product ${populatedProductUpdate.title}`);
          break;

        case 'delete':
          await ProductCache.deleteOne({ _id: change.documentKey._id });
          console.log(`Retried delete for product ID ${change.documentKey._id}`);
          break;

        default:
          console.log('Unrecognized change event in retry:', change.operationType);
      }
    } catch (err) {
      console.error('Retry operation failed, adding back to queue:', err.message);
      retryQueue.push(change); // Re-add the change to the queue for the next retry
    }
  }
};

// Helper function to prepare ProductCache data
const populateProductCacheData = (product) => ({
  title: product.title,
  description: product.description,
  category_id: product.category_id ? product.category_id._id.toString() : null,
  category: product.category_id ? product.category_id.name : null, // Extract category name
  price: product.price,
  quantity: product.quantity,
  image: product.image,
  imageId: product.imageId,
  seller: product.seller,
});

module.exports = syncProductCache;
