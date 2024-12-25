// orders microservice productCacheModel.js
const mongoose = require("mongoose");

const productCacheSchema = new mongoose.Schema({
  _id: { type: mongoose.Schema.Types.ObjectId, required: true },
  title: { type: String, required: true },
  description: { type: String },
  category_id: { type: String },
  price: { type: Number, required: true },
  quantity: { type: Number},
  image: { type: String }, 
  imageId: { type: mongoose.Schema.Types.ObjectId, ref: 'fs.files' },
  updatedAt: { type: Date, default: Date.now },
  category: { type: String },
  seller: {
    id: { type: String, required: true }
  }

});


module.exports = mongoose.model("ProductCache", productCacheSchema);