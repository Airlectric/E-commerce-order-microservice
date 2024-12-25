const mongoose = require('mongoose');
const ProductCache = require("../models/productCacheModel");
const { searchProducts } = require('../services/elasticsearchSync'); 


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

    // Return product data without processing images
    res.status(200).json(product);
  } catch (err) {
    console.error("Error in getProductById:", err.message);
    res.status(500).json({ message: err.message });
  }
};


// Search products API endpoint
exports.search = async (req, res) => {
  try {
    const query = req.query;

    // Validate price filters
    if (query.price) {
      query.price = {
        min: parseFloat(query.price.min) || 0,
        max: parseFloat(query.price.max) || Number.MAX_VALUE,
      };
    }

    console.log('Validated query parameters:', query);

    // Perform the search
    const results = await searchProducts(query);
    res.json(results);
  } catch (error) {
    console.error('Error in search API:', error);
    res.status(500).send({ error: 'Error during search', details: error.message });
  }
};
