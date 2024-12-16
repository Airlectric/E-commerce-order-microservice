// orderModel.js

const mongoose = require('mongoose');


const orderSchema = new mongoose.Schema(
  {
    user: {
      id: { type: String, required: true }, // User ID from JWT
      profileUrl: { type: String }, // Optional profile URL
    },
    products: [
      {
        productId: { type: mongoose.Schema.Types.ObjectId, ref: 'Product', required: true },
        quantity: { type: Number, required: true },
      },
    ],
    totalAmount: { type: Number, required: true },
    status: { type: String, enum: ['Pending', 'Shipped', 'Delivered', 'Cancelled'], default: 'Pending' },
  },
  { timestamps: true }
);

module.exports = mongoose.model('Order', orderSchema);

