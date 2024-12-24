const mongoose = require('mongoose');
const Grid = require('gridfs-stream');
const { connectOrderDB } = require("../config/db");
const ProductCache = require("../models/productCacheModel");

let gfs;
const conn = mongoose.connection;

// Set up GridFS after DB connection is established
mongoose.connection.once('open', () => {
  gfs = Grid(conn.db, mongoose.mongo);
  gfs.collection('fs'); // GridFS collection
  console.log('GridFS connected');
});

// Get all products
exports.getAllProducts = async (req, res) => {
  try {
    const products = await ProductCache.find();
    res.status(200).json(products);
  } catch (err) {
    console.error("Error in getAllProducts:", err.message);
    res.status(500).json({ message: err.message });
  }
};

// Get product by ID
exports.getProductById = async (req, res) => {
  try {
    const product = await ProductCache.findById(req.params.id);

    if (!product) {
      return res.status(404).json({ message: "Product not found" });
    }

    // Check if the product has an associated image
    if (product.imageId) {
      const file = await gfs.files.findOne({ _id: product.imageId });

      if (!file || file.contentType.startsWith('image/') === false) {
        return res.status(404).json({ message: "Image not found or invalid format" });
      }

      // Stream the image file
      const readStream = gfs.createReadStream({ _id: file._id });
      res.set('Content-Type', file.contentType);
      return readStream.pipe(res);
    }

    // No image; return product data
    res.status(200).json(product);
  } catch (err) {
    console.error("Error in getProductById:", err.message);
    res.status(500).json({ message: err.message });
  }
};
