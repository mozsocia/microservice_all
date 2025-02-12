Below is a complete example of the project with full code for all files. You can copy and paste these files into your project folders. This example assumes the following structure:

```
/my-microservices-app
├── api-gateway
│   ├── package.json
│   └── index.js
├── order-service
│   ├── package.json
│   ├── index.js
│   ├── config
│   │   └── database.js
│   ├── controllers
│   │   └── orderController.js
│   ├── models
│   │   └── order.js
│   └── // (optional) add a subscriber file if the order service needs to consume events
├── product-service
│   ├── package.json
│   ├── index.js
│   ├── config
│   │   └── database.js
│   ├── controllers
│   │   └── productController.js
│   ├── models
│   │   └── product.js
│   └── rabbitmq
│       └── subscriber.js
└── shared
    └── rabbitmq
        └── rabbitmq.js
```

> **Note:**  
> - You can use [dotenv](https://www.npmjs.com/package/dotenv) to manage environment variables (like `MONGODB_URI`, `PORT`, or `RABBITMQ_URL`). For simplicity, the following examples use fallback defaults.  
> - Ensure that you install the required npm packages for each service.

---

## 1. API Gateway

### File: `api-gateway/package.json`

```json
{
  "name": "api-gateway",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": {
    "start": "node index.js"
  },
  "dependencies": {
    "express": "^4.18.2",
    "http-proxy-middleware": "^2.0.6"
  }
}
```

### File: `api-gateway/index.js`

```javascript
// api-gateway/index.js
const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');

const app = express();
app.use(express.json());

// Proxy requests to Product Service (assumed to run on port 3001)
app.use('/products', createProxyMiddleware({
  target: 'http://localhost:3001',
  changeOrigin: true,
  pathRewrite: { '^/products': '' }
}));

// Proxy requests to Order Service (assumed to run on port 3002)
app.use('/orders', createProxyMiddleware({
  target: 'http://localhost:3002',
  changeOrigin: true,
  pathRewrite: { '^/orders': '' }
}));

// A simple health-check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'API Gateway is running' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`API Gateway listening on port ${PORT}`));
```

---

## 2. Product Service

### File: `product-service/package.json`

```json
{
  "name": "product-service",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": {
    "start": "node index.js"
  },
  "dependencies": {
    "express": "^4.18.2",
    "mongoose": "^7.0.0",
    "amqplib": "^0.10.3"
  }
}
```

### File: `product-service/config/database.js`

```javascript
// product-service/config/database.js
const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/product-service';

const connectDB = async () => {
  try {
    await mongoose.connect(MONGODB_URI);
    console.log('Product Service: MongoDB connected.');
  } catch (error) {
    console.error('Product Service: Error connecting to MongoDB', error);
    process.exit(1);
  }
};

module.exports = connectDB;
```

### File: `product-service/models/product.js`

```javascript
// product-service/models/product.js
const mongoose = require('mongoose');

const productSchema = new mongoose.Schema({
  name: { type: String, required: true },
  description: String,
  price: { type: Number, required: true },
  // The quantity available in stock.
  quantity: { type: Number, required: true, default: 0 },
  createdAt: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Product', productSchema);
```

### File: `product-service/controllers/productController.js`

```javascript
// product-service/controllers/productController.js
const Product = require('../models/product');
const rabbitmq = require('../../shared/rabbitmq/rabbitmq');

exports.createProduct = async (req, res) => {
  try {
    const { name, description, price, quantity } = req.body;
    const product = new Product({ name, description, price, quantity });
    await product.save();

    // Optionally publish an event for product creation
    await rabbitmq.publish('product.created', { productId: product._id, name });
    
    res.status(201).json(product);
  } catch (error) {
    console.error('Product Service: Error creating product:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

exports.getProducts = async (req, res) => {
  try {
    const products = await Product.find();
    res.json(products);
  } catch (error) {
    console.error('Product Service: Error retrieving products:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};
```

### File: `product-service/rabbitmq/subscriber.js`

```javascript
// product-service/rabbitmq/subscriber.js
const rabbitmq = require('../../shared/rabbitmq/rabbitmq');
const Product = require('../models/product');

// Subscribe to 'order.created' events to update product quantity.
rabbitmq.subscribe('order.created', async (message, routingKey) => {
  console.log(`Product Service: Received ${routingKey} event:`, message);
  
  // Expecting message to contain productId and quantity (ordered quantity)
  const { productId, quantity: orderedQuantity } = message;
  
  try {
    const product = await Product.findById(productId);
    if (product) {
      // Deduct the ordered quantity from the product's stock.
      if (product.quantity >= orderedQuantity) {
        product.quantity -= orderedQuantity;
      } else {
        console.warn(`Product Service: Insufficient stock for product ${productId}. Current stock: ${product.quantity}, Ordered: ${orderedQuantity}`);
        product.quantity = 0;
      }
      await product.save();
      console.log(`Product Service: Updated product ${productId} quantity to ${product.quantity}`);
    } else {
      console.error(`Product Service: Product ${productId} not found.`);
    }
  } catch (error) {
    console.error(`Product Service: Error updating product quantity for product ${productId}:`, error);
  }
});
```

### File: `product-service/index.js`

```javascript
// product-service/index.js
const express = require('express');
const connectDB = require('./config/database');
const productController = require('./controllers/productController');

// Ensure the subscriber is loaded so that it starts listening.
require('./rabbitmq/subscriber');

const app = express();
app.use(express.json());

// Connect to MongoDB.
connectDB();

// Define endpoints (no separate route files per your requirement)
app.post('/products', productController.createProduct);
app.get('/products', productController.getProducts);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Product Service listening on port ${PORT}`));
```

---

## 3. Order Service

### File: `order-service/package.json`

```json
{
  "name": "order-service",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": {
    "start": "node index.js"
  },
  "dependencies": {
    "express": "^4.18.2",
    "mongoose": "^7.0.0",
    "amqplib": "^0.10.3"
  }
}
```

### File: `order-service/config/database.js`

```javascript
// order-service/config/database.js
const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/order-service';

const connectDB = async () => {
  try {
    await mongoose.connect(MONGODB_URI);
    console.log('Order Service: MongoDB connected.');
  } catch (error) {
    console.error('Order Service: Error connecting to MongoDB', error);
    process.exit(1);
  }
};

module.exports = connectDB;
```

### File: `order-service/models/order.js`

```javascript
// order-service/models/order.js
const mongoose = require('mongoose');

const orderSchema = new mongoose.Schema({
  // Reference to the Product (assumes that product _id is stored here)
  productId: { type: mongoose.Schema.Types.ObjectId, ref: 'Product', required: true },
  quantity: { type: Number, required: true },
  orderDate: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Order', orderSchema);
```

### File: `order-service/controllers/orderController.js`

```javascript
// order-service/controllers/orderController.js
const Order = require('../models/order');
const rabbitmq = require('../../shared/rabbitmq/rabbitmq');

exports.createOrder = async (req, res) => {
  try {
    const { productId, quantity } = req.body;
    const order = new Order({ productId, quantity });
    await order.save();

    // Publish an event for order creation with productId and quantity.
    await rabbitmq.publish('order.created', { orderId: order._id, productId, quantity });
    
    res.status(201).json(order);
  } catch (error) {
    console.error('Order Service: Error creating order:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

exports.getOrders = async (req, res) => {
  try {
    // Populate product details if needed.
    const orders = await Order.find().populate('productId');
    res.json(orders);
  } catch (error) {
    console.error('Order Service: Error retrieving orders:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};
```

### File: `order-service/index.js`

```javascript
// order-service/index.js
const express = require('express');
const connectDB = require('./config/database');
const orderController = require('./controllers/orderController');

const app = express();
app.use(express.json());

// Connect to MongoDB.
connectDB();

// Define endpoints (no separate route files per your requirement)
app.post('/orders', orderController.createOrder);
app.get('/orders', orderController.getOrders);

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => console.log(`Order Service listening on port ${PORT}`));
```

---

## 4. Shared RabbitMQ Utility

### File: `shared/rabbitmq/rabbitmq.js`

```javascript
// shared/rabbitmq/rabbitmq.js
const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
const EXCHANGE_NAME = 'microservices_topic_exchange';
const EXCHANGE_TYPE = 'topic';

let channel = null;

async function init() {
  if (channel) return channel;
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, { durable: true });
    console.log('RabbitMQ: Channel initialized.');
    return channel;
  } catch (error) {
    console.error('RabbitMQ: Error initializing channel', error);
    throw error;
  }
}

/**
 * Publishes a message with a specific routing key.
 * @param {string} routingKey - e.g., 'order.created' or 'product.created'
 * @param {Object} message - The message payload.
 */
async function publish(routingKey, message) {
  try {
    const ch = await init();
    const payload = Buffer.from(JSON.stringify(message));
    ch.publish(EXCHANGE_NAME, routingKey, payload);
    console.log(`RabbitMQ: Published event with key '${routingKey}'`);
  } catch (error) {
    console.error('RabbitMQ: Error publishing message', error);
  }
}

/**
 * Subscribes to messages matching a given binding key.
 * @param {string} bindingKey - e.g., 'order.created' or '#' for all events.
 * @param {Function} onMessage - Callback to process the incoming message.
 */
async function subscribe(bindingKey, onMessage) {
  try {
    const ch = await init();
    // Create an exclusive queue for this consumer.
    const q = await ch.assertQueue('', { exclusive: true });
    await ch.bindQueue(q.queue, EXCHANGE_NAME, bindingKey);
    console.log(`RabbitMQ: Subscribed to events with binding key '${bindingKey}'`);
    
    ch.consume(q.queue, (msg) => {
      if (msg !== null) {
        const payload = JSON.parse(msg.content.toString());
        onMessage(payload, msg.fields.routingKey);
        ch.ack(msg);
      }
    });
  } catch (error) {
    console.error('RabbitMQ: Error subscribing to messages', error);
  }
}

module.exports = {
  init,
  publish,
  subscribe
};
```

---

## Final Setup and Running

1. **Install Dependencies:**  
   For each service and the API Gateway, navigate to the folder and run:

   ```bash
   npm install
   ```

2. **Run RabbitMQ:**  
   Ensure RabbitMQ is running locally or update `RABBITMQ_URL` as needed.

3. **Start Each Service:**  
   - In the `api-gateway` folder:  
     ```bash
     npm start
     ```
   - In the `product-service` folder:  
     ```bash
     npm start
     ```
   - In the `order-service` folder:  
     ```bash
     npm start
     ```

4. **Testing the Setup:**  
   - **Create a Product:**  
     Send a POST request to `http://localhost:3001/products` (or through the API Gateway at `http://localhost:3000/products`) with JSON:
     
     ```json
     {
       "name": "Example Product",
       "description": "A test product",
       "price": 99.99,
       "quantity": 50
     }
     ```
     
   - **Create an Order:**  
     Send a POST request to `http://localhost:3002/orders` (or via API Gateway at `http://localhost:3000/orders`) with JSON:
     
     ```json
     {
       "productId": "<insert product id>",
       "quantity": 5
     }
     ```
     
     The Order Service will publish an `order.created` event, and the Product Service subscriber will receive it and decrease the product's quantity accordingly.

This complete code setup demonstrates a Node.js microservices architecture with an API Gateway, RabbitMQ-based event-driven communication (using a topic exchange), and inter-service logic (decreasing product quantity on order events). Adjust environment variables, error handling, and logging as needed for your production requirements.