const express = require('express');
const mongoose = require('mongoose');

const router = express.Router();


// MongoDB Connection URI
const mongoURI = process.env.MONGO_URI_PRODUCT;

// Create a Mongoose connection
const conn = mongoose.createConnection(mongoURI);

// Initialize GridFSBucket
let gfs;
conn.once('open', () => {
  gfs = new mongoose.mongo.GridFSBucket(conn.db, {
    bucketName: 'uploads', // This should match the bucketName in your Multer configuration
  });
});


// Stream image files by filename
router.get('/uploads/:filename', async (req, res) => {
  try {
    // Check if the file exists in GridFS
    const files = await gfs.find({ filename: req.params.filename }).toArray();
    if (!files || files.length === 0) {
      return res.status(404).json({ message: 'File not found' });
    }

    // Stream file from GridFS
    const readStream = gfs.openDownloadStreamByName(req.params.filename);
    readStream.on('error', (error) => {
      res.status(500).json({ message: 'Error streaming file', error: error.message });
    });
    readStream.pipe(res);
  } catch (err) {
    console.error('Error streaming file:', err);
    res.status(500).json({ message: 'Failed to fetch file', error: err.message });
  }
});


module.exports = router;