const mongoose = require('mongoose');

const productSchema = new mongoose.Schema(
  {
    title: { type: String, required: true },
    description: { type: String, required: true },
    price: { type: Number, required: true },
    category_id: { type: mongoose.Schema.Types.ObjectId, ref: 'Category' },
    quantity: {type: Number, required: true},
    image: { type: String }, // URL for image (optional)
    imageId: { type: mongoose.Schema.Types.ObjectId, ref: 'fs.files' }, // GridFS ID for actual image
    seller: {
      id: { type: String, required: true }, // Seller ID from JWT
      profileUrl: { type: String }, // Profile URL (optional)
      profileImageId: { type: mongoose.Schema.Types.ObjectId, ref: 'fs.files' }, // GridFS ID for profile image
    },
  },
  { timestamps: true }
);

module.exports = mongoose.model('Product', productSchema);