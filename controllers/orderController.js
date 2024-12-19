const Order = require("../models/orderModel");
const { sendMessage } = require("../config/rabbitmq");
const elasticsearch = require("elasticsearch");
const { connectProductCacheDB } = require("../config/db");

let ProductCache;

// Initialize ProductCache with a dedicated connection
(async () => {
  const productCacheConnection = await connectProductCacheDB();
  ProductCache = require("../models/productCacheModel")(productCacheConnection);
})();

// Set up Elasticsearch client
const esClient = new elasticsearch.Client({
  host: process.env.ELASTICSEARCH_URL,
});

// Helper function for product validation and total calculation
const validateProductsAndCalculateTotal = async (products) => {
  let totalAmount = 0;
  const productDetails = [];
  const inventoryUpdates = [];
  let productCount = 0;

  for (const product of products) {
    const productRecord = await ProductCache.findById(product.productId);
    if (!productRecord) {
      throw new Error(`Product with ID ${product.productId} not found`);
    }

    if (productRecord.quantity < product.quantity) {
      throw new Error(`Insufficient quantity for product ${productRecord.title}`);
    }

    productDetails.push({
      productId: product.productId,
      quantity: product.quantity,
      title: productRecord.title,
      sellerId: productRecord.seller.id,
    });

    inventoryUpdates.push({
      productId: product.productId,
      quantity: -product.quantity, // Deduction for inventory
    });

    totalAmount += product.quantity * productRecord.price;
    productCount += product.quantity;
  }

  return { totalAmount, productDetails, inventoryUpdates, productCount };
};

// Create an order
exports.createOrder = async (req, res) => {
  try {
    const { products } = req.body;

    if (!products || products.length === 0) {
      return res.status(400).json({ message: "Products are required" });
    }

    const { totalAmount, productDetails, inventoryUpdates, productCount } =
      await validateProductsAndCalculateTotal(products);
	
	

    const order = new Order({
      user: { id: req.user.user_id, profileUrl: req.user.profileUrl },
      products: productDetails.map(({ productId, quantity }) => ({ productId, quantity })),
      totalAmount,
    });

    await order.save();

    // Send inventory updates first
    sendMessage("update_inventory", inventoryUpdates);

    // Set a 5-second delay for the order events notification
    setTimeout(() => {
      sendMessage("order_events_for_notifications", {
        type: "order_placed",
        data: {
          orderId: order.id,
          userId: req.user.user_id,
          totalProducts: productCount,
          productIds: productDetails.map(({ productId }) => productId),
          quantities: productDetails.map(({ quantity }) => quantity),
          sellerIds: productDetails.map(({ sellerId }) => sellerId),
          titles: productDetails.map(({ title }) => title),
        },
      });
    }, 5000);

    // Index the order in Elasticsearch
    await esClient.index({
      index: "orders",
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

    res.status(201).json(order);
  } catch (err) {
    console.error("Error in createOrder:", err.message);
    res.status(500).json({ message: err.message });
  }
};

// Get all orders for a user
exports.getOrders = async (req, res) => {
  try {
    const orders = await Order.find({ "user.id": req.user.user_id });
    res.json(orders);
  } catch (err) {
    console.error("Error in getOrders:", err.message);
    res.status(500).json({ message: err.message });
  }
};

// Get an order by ID
exports.getOrderById = async (req, res) => {
  try {
    const order = await Order.findById(req.params.id);

    if (!order) {
      return res.status(404).json({ message: "Order not found" });
    }

    if (String(order.user.id) !== String(req.user.user_id)) {
      return res.status(403).json({ message: "Unauthorized" });
    }

    res.json(order);
  } catch (err) {
    console.error("Error in getOrderById:", err.message);
    res.status(500).json({ message: err.message });
  }
};

// Update an order
exports.updateOrder = async (req, res) => {
  try {
    const { products } = req.body;
    const order = await Order.findById(req.params.id);

    if (!order) {
      return res.status(404).json({ message: "Order not found" });
    }

    if (String(order.user.id) !== String(req.user.user_id)) {
      return res.status(403).json({ message: "Unauthorized" });
    }

    if (order.status !== "Pending") {
      return res.status(400).json({ message: "Only pending orders can be updated" });
    }

    if (!products || products.length === 0) {
      return res.status(400).json({ message: "Products are required" });
    }

    const previousProducts = order.products;

    const { totalAmount, productDetails, inventoryUpdates } =
      await validateProductsAndCalculateTotal(products);

    // Calculate inventory reversals for the previous state
    const reverseInventoryUpdates = previousProducts.map((prevProduct) => ({
      productId: prevProduct.productId,
      quantity: prevProduct.quantity, // Return previous quantities
    }));

    order.products = productDetails;
    order.totalAmount = totalAmount;

    await order.save();

    // Send inventory updates first
    sendMessage("update_inventory", [...reverseInventoryUpdates, ...inventoryUpdates]);

    // Set a 5-second delay for the order events notification
    setTimeout(() => {
      sendMessage("order_events_for_notifications", {
        type: "order_updated",
        data: {
          orderId: order.id,
          userId: req.user.user_id,
          totalProducts: productDetails.length,
          productIds: productDetails.map(({ productId }) => productId),
          quantities: productDetails.map(({ quantity }) => quantity),
          sellerIds: productDetails.map(({ sellerId }) => sellerId),
          titles: productDetails.map(({ title }) => title),
        },
      });
    }, 5000);

    // Update Elasticsearch
    await esClient.update({
      index: "orders",
      id: order.id,
      body: { doc: { products: order.products, totalAmount: order.totalAmount } },
    });

    res.json(order);
  } catch (err) {
    console.error("Error in updateOrder:", err.message);
    res.status(500).json({ message: err.message });
  }
};

// Delete an order
exports.deleteOrder = async (req, res) => {
  try {
    const order = await Order.findById(req.params.id);

    if (!order) {
      return res.status(404).json({ message: "Order not found" });
    }

    if (String(order.user.id) !== String(req.user.user_id)) {
      return res.status(403).json({ message: "Unauthorized" });
    }

    if (order.status !== "Pending") {
      return res.status(400).json({ message: "Only pending orders can be deleted" });
    }

    // Fetch detailed product information for deleted order
    const productDetails = await Promise.all(
      order.products.map(async (product) => {
        const productRecord = await ProductCache.findById(product.productId);

        return productRecord
          ? {
              productId: product.productId,
              quantity: product.quantity,
              sellerId: productRecord.seller.id, // Fetch sellerId
              title: productRecord.title,        // Fetch title
            }
          : {
              productId: product.productId,
              quantity: product.quantity,
              sellerId: null,
              title: null,
            };
      })
    );

    const reverseInventoryUpdates = productDetails.map((product) => ({
      productId: product.productId,
      quantity: product.quantity, // Restock all products
    }));

    // Delete the order using `deleteOne` or `findByIdAndDelete`
    await Order.deleteOne({ _id: req.params.id });

    // Send inventory updates first
    sendMessage("update_inventory", reverseInventoryUpdates);

    // Set a 5-second delay for the order events notification
    setTimeout(() => {
      sendMessage("order_events_for_notifications", {
        type: "order_deleted",
        data: {
          orderId: order.id,
          userId: req.user.user_id,
          totalProducts: productDetails.length,
          productIds: productDetails.map(({ productId }) => productId),
          quantities: productDetails.map(({ quantity }) => quantity),
          sellerIds: productDetails.map(({ sellerId }) => sellerId),
          titles: productDetails.map(({ title }) => title),
        },
      });
    }, 5000);

    // Remove Elasticsearch entry
    await esClient.delete({
      index: "orders",
      id: order.id,
    });

    res.json({ message: "Order deleted successfully" });
  } catch (err) {
    console.error("Error in deleteOrder:", err.message);
    res.status(500).json({ message: err.message });
  }
};

// Update order status (Admin only)
exports.updateOrderStatus = async (req, res) => {
  try {
    const { status } = req.body;
    const order = await Order.findById(req.params.id);

    if (!order) {
      return res.status(404).json({ message: "Order not found" });
    }

    if (!["Pending", "Shipped", "Delivered", "Cancelled"].includes(status)) {
      return res.status(400).json({ message: "Invalid status" });
    }

    order.status = status;
    await order.save();

    // Send order status update to RabbitMQ for notifications
    sendMessage("order_status_updated", { orderId: order.id, status });

    // Update the order in Elasticsearch
    await esClient.update({
      index: "orders",
      id: order.id,
      body: { doc: { status } },
    });

    res.json(order);
  } catch (err) {
    console.error("Error in updateOrderStatus:", err.message);
    res.status(500).json({ message: err.message });
  }
};
