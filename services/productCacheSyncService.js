const mongoose = require("mongoose");
const ProductCacheModel = require("../models/productCacheModel");
const { connectProductCacheDB } = require("../config/db");
require('dotenv').config();

// MongoDB Synchronization for ProductCache
const syncProductCache = async () => {
  try {
    const productCacheConnection = await connectProductCacheDB();
    const ProductCache = ProductCacheModel(productCacheConnection);

    // Connect to Product DB
    const productDB = await mongoose.createConnection(process.env.MONGO_URI_PRODUCT);
    const ProductModel = productDB.model("Product", new mongoose.Schema({
      // Define your product schema here if needed
      title: String,
      description: String,
      category_id: String,
      price: Number,
      quantity: Number,
      category: String,
      seller: String
    }));

    console.log("Listening to Product collection for changes...");

    // Start watching Change Streams on the product collection
    const changeStream = ProductModel.watch([], { fullDocument: "updateLookup" });

    changeStream.on("change", async (change) => {
      try {
        console.log("Change detected in Product Collection:", change);

        switch (change.operationType) {
          case "insert": {
            const newProduct = change.fullDocument;

            // Insert into ProductCache
            await ProductCache.create({
              _id: newProduct._id,
              title: newProduct.title,
              description: newProduct.description,
              category_id: newProduct.category_id,
              price: newProduct.price,
              quantity: newProduct.quantity,
              updatedAt: new Date(),
              category: newProduct.category,
              seller: newProduct.seller
            });
            console.log(`Inserted product ${newProduct.title} into ProductCache`);
            break;
          }

          case "update": {
            const updatedProduct = change.fullDocument;

            // Update ProductCache
            await ProductCache.updateOne(
              { _id: updatedProduct._id },
              {
                $set: {
                  title: updatedProduct.title,
                  description: updatedProduct.description,
                  category_id: updatedProduct.category_id,
                  price: updatedProduct.price,
                  quantity: updatedProduct.quantity,
                  updatedAt: new Date(),
                  category: updatedProduct.category,
                  seller: updatedProduct.seller
                }
              },
              { upsert: true }
            );
            console.log(`Updated product ${updatedProduct.title} in ProductCache`);
            break;
          }

          case "delete": {
            const deletedProductId = change.documentKey._id;

            // Delete from ProductCache
            await ProductCache.deleteOne({ _id: deletedProductId });
            console.log(`Deleted product with ID ${deletedProductId} from ProductCache`);
            break;
          }

          default:
            console.log("Unrecognized change event:", change.operationType);
        }
      } catch (err) {
        console.error("Error processing product change stream:", err.message);
      }
    });

    changeStream.on("error", (err) => {
      console.error("Error in change stream:", err.message);
    });
  } catch (err) {
    console.error("Error starting Product Cache Sync:", err.message);
  }
};

module.exports = syncProductCache;
