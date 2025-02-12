Below is an updated version of the **Product Service** that separates each event handler into its own file. This approach improves maintainability and scalability as your number of events grows. In this example, we use a dedicated directory for event handlers and an aggregator (an `index.js` file) that automatically loads every handler file. The subscription manager then registers each event handler with RabbitMQ.

Below is the complete code for the **Product Service** with the new events structure.

---

## **Updated Project Structure (Product Service)**

```
/product-service
├── config
│   └── database.js
├── controllers
│   └── productController.js
├── events
│   ├── handlers
│   │   ├── orderCreated.js
│   │   ├── productUpdated.js      // (Example: add other events similarly)
│   │   └── index.js               // Aggregates all event handlers
│   └── subscriptions.js         // Registers all event subscriptions
├── models
│   └── product.js
├── utils
│   └── logger.js
├── index.js
└── package.json
```

> **Note:**  
> - You can add as many event handler files under `/events/handlers` as needed.  
> - The aggregator file (`/events/handlers/index.js`) automatically collects all modules that export an object with `event` and `handler` properties.

---

## **1. Configuration & Database Connection**

**File: `product-service/config/database.js`**

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

---

## **2. Data Model**

**File: `product-service/models/product.js`**

```javascript
// product-service/models/product.js
const mongoose = require('mongoose');

const productSchema = new mongoose.Schema({
  name: { type: String, required: true },
  description: String,
  price: { type: Number, required: true },
  // Stock quantity for the product.
  quantity: { type: Number, required: true, default: 0 },
  createdAt: { type: Date, default: Date.now }
});

module.exports = mongoose.model('Product', productSchema);
```

---

## **3. Controller**

**File: `product-service/controllers/productController.js`**

```javascript
// product-service/controllers/productController.js
const Product = require('../models/product');
const rabbitmq = require('../../shared/rabbitmq/rabbitmq');
const logger = require('../utils/logger');

exports.createProduct = async (req, res) => {
  try {
    const { name, description, price, quantity } = req.body;
    const product = new Product({ name, description, price, quantity });
    await product.save();

    // Optionally publish an event (for auditing, caching, etc.)
    await rabbitmq.publish('product.created', { productId: product._id, name });
    logger.info(`Created product ${product._id} (${name})`);

    res.status(201).json(product);
  } catch (error) {
    logger.error('Error creating product:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

exports.getProducts = async (req, res) => {
  try {
    const products = await Product.find();
    res.json(products);
  } catch (error) {
    logger.error('Error retrieving products:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};
```

---

## **4. Event Handlers**

Each event handler is defined in its own file under the `/events/handlers` directory.

### **a. Order Created Event Handler**

**File: `product-service/events/handlers/orderCreated.js`**

```javascript
// product-service/events/handlers/orderCreated.js
const Product = require('../../models/product');
const logger = require('../../utils/logger');

module.exports = {
  event: 'order.created',
  handler: async (message, routingKey) => {
    logger.info(`Handling '${routingKey}' event in orderCreated.js`);
    const { productId, quantity: orderedQuantity } = message;
    try {
      const product = await Product.findById(productId);
      if (!product) {
        logger.error(`Product with ID ${productId} not found.`);
        return;
      }
      if (product.quantity >= orderedQuantity) {
        product.quantity -= orderedQuantity;
        logger.info(`Decreased product ${productId} stock by ${orderedQuantity}.`);
      } else {
        logger.warn(`Insufficient stock for product ${productId}. Setting quantity to 0.`);
        product.quantity = 0;
      }
      await product.save();
      logger.info(`Updated product ${productId} quantity: ${product.quantity}`);
    } catch (err) {
      logger.error(`Error processing '${routingKey}' event: ${err}`);
    }
  }
};
```

### **b. Product Updated Event Handler (Example)**

**File: `product-service/events/handlers/productUpdated.js`**

```javascript
// product-service/events/handlers/productUpdated.js
const logger = require('../../utils/logger');

module.exports = {
  event: 'product.updated',
  handler: async (message, routingKey) => {
    logger.info(`Handling '${routingKey}' event in productUpdated.js with message:`, message);
    // Implement your product update logic here.
  }
};
```

### **c. Aggregator for Event Handlers**

This file automatically loads every handler file from the `/events/handlers` directory.

**File: `product-service/events/handlers/index.js`**

```javascript
// product-service/events/handlers/index.js
const fs = require('fs');
const path = require('path');

const handlers = {};

// Read every .js file (except this file) in the current directory.
fs.readdirSync(__dirname)
  .filter(file => file !== 'index.js' && file.endsWith('.js'))
  .forEach(file => {
    const handlerModule = require(path.join(__dirname, file));
    if (handlerModule.event && handlerModule.handler) {
      handlers[handlerModule.event] = handlerModule.handler;
    }
  });

module.exports = handlers;
```

---

## **5. Subscription Manager**

This file registers all event handlers with RabbitMQ. It reads the aggregated handlers from the `handlers/index.js` file.

**File: `product-service/events/subscriptions.js`**

```javascript
// product-service/events/subscriptions.js
const rabbitmq = require('../../shared/rabbitmq/rabbitmq');
const eventHandlers = require('./handlers'); // Aggregated event handlers
const logger = require('../utils/logger');

const registerEventHandlers = async () => {
  for (const event in eventHandlers) {
    if (Object.prototype.hasOwnProperty.call(eventHandlers, event)) {
      try {
        await rabbitmq.subscribe(event, async (message, routingKey) => {
          try {
            await eventHandlers[event](message, routingKey);
          } catch (err) {
            logger.error(`Error processing event '${routingKey}':`, err);
          }
        });
        logger.info(`Subscribed to event '${event}'`);
      } catch (err) {
        logger.error(`Failed to subscribe to event '${event}':`, err);
      }
    }
  }
};

module.exports = registerEventHandlers;
```

---

## **6. Logger Utility**

A simple logger that wraps standard console methods for consistent logging.

**File: `product-service/utils/logger.js`**

```javascript
// product-service/utils/logger.js
const info = (msg, ...args) => console.log(`[INFO] ${msg}`, ...args);
const warn = (msg, ...args) => console.warn(`[WARN] ${msg}`, ...args);
const error = (msg, ...args) => console.error(`[ERROR] ${msg}`, ...args);

module.exports = {
  info,
  warn,
  error,
};
```

---

## **7. Service Entry Point**

The main file initializes the Express app, connects to the database, and registers all event subscriptions.

**File: `product-service/index.js`**

```javascript
// product-service/index.js
const express = require('express');
const connectDB = require('./config/database');
const productController = require('./controllers/productController');
const registerEventHandlers = require('./events/subscriptions');
const logger = require('./utils/logger');

const app = express();
app.use(express.json());

// Connect to MongoDB.
connectDB();

// Register RabbitMQ event handlers.
registerEventHandlers()
  .then(() => logger.info('All event handlers registered.'))
  .catch(err => logger.error('Error registering event handlers:', err));

// Define API endpoints.
app.post('/products', productController.createProduct);
app.get('/products', productController.getProducts);

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => logger.info(`Product Service listening on port ${PORT}`));
```

---

## **8. Shared RabbitMQ Utility**

Make sure the shared RabbitMQ utility is available to all services. For reference, here’s the code:

**File: `shared/rabbitmq/rabbitmq.js`**

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

## **Usage**

1. **Install Dependencies:**  
   Navigate to your `product-service` folder and run:

   ```bash
   npm install
   ```

2. **Start the Service:**  
   From the `product-service` folder, run:

   ```bash
   npm start
   ```

3. **Add More Events:**  
   To subscribe to additional events, add a new file under `/events/handlers` that exports an object with an `event` key (the event name) and a `handler` function. The subscription manager will automatically load and subscribe to it.

This modular, production-quality setup allows you to manage each event in its own file while keeping your code organized and maintainable as your microservice grows.