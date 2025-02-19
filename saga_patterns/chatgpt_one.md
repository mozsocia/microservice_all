Below is a step‐by‐step, beginner‐friendly example of implementing the Saga pattern in a microservices architecture using Node.js, RabbitMQ, and MongoDB. In our example, we simulate an order flow where an order is created and then processed through payment, inventory, and delivery services. If any step fails, a compensation (rollback) event is triggered.

> **Overview of the Flow (Saga):**
>
> 1. **Order Service:**  
>    - Receives a new order request (via an HTTP API).  
>    - Saves the order in MongoDB.  
>    - Publishes an `order_created` event to RabbitMQ.
>
> 2. **Payment Service:**  
>    - Subscribes to `order_created` events.  
>    - Processes the payment (with a simulated chance of failure).  
>    - Saves payment details in MongoDB.  
>    - Publishes a `payment_success` event on success **or** a `payment_failed` event on failure.
>
> 3. **Inventory Service:**  
>    - Listens for `payment_success` events.  
>    - Checks/reserves inventory in MongoDB.  
>    - Publishes an `inventory_reserved` event if successful **or** an `inventory_failed` event if not.
>
> 4. **Delivery Service:**  
>    - Listens for `inventory_reserved` events.  
>    - Schedules delivery (stored in MongoDB) and publishes a `delivery_scheduled` event.
>
> 5. **Compensation:**  
>    - The **Order Service** listens for compensation events (like `payment_failed` or `inventory_failed`) to cancel the order if needed.
>
> Each service communicates solely via events, making the system loosely coupled.

---

## Project Structure

```
saga-example/
└── services/
    ├── order-service/
    │   ├── orderService.js
    │   ├── rabbitmq.js
    │   └── package.json
    ├── payment-service/
    │   ├── paymentService.js
    │   ├── rabbitmq.js
    │   └── package.json
    ├── inventory-service/
    │   ├── inventoryService.js
    │   ├── rabbitmq.js
    │   └── package.json
    └── delivery-service/
        ├── deliveryService.js
        ├── rabbitmq.js
        └── package.json
```

*For simplicity, each service uses its own copy of a helper file (`rabbitmq.js`) to manage RabbitMQ connections and messaging. In a real-world scenario, you might share this library across services.*

---

## Shared RabbitMQ Helper (`rabbitmq.js`)

Create a file named `rabbitmq.js` in each service with the following content:

```js
// rabbitmq.js
const amqp = require('amqplib');

let channel, connection;

async function connect() {
  connection = await amqp.connect('amqp://localhost');
  channel = await connection.createChannel();
  return channel;
}

async function publish(queue, message) {
  if (!channel) await connect();
  await channel.assertQueue(queue, { durable: true });
  channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), { persistent: true });
  console.log(`Message sent to queue "${queue}": ${JSON.stringify(message)}`);
}

async function subscribe(queue, callback) {
  if (!channel) await connect();
  await channel.assertQueue(queue, { durable: true });
  channel.consume(queue, (msg) => {
    if (msg !== null) {
      const message = JSON.parse(msg.content.toString());
      callback(message);
      channel.ack(msg);
    }
  });
}

module.exports = { connect, publish, subscribe };
```

---

## 1. Order Service

This service provides an HTTP endpoint to create orders and publishes an `order_created` event. It also listens for compensation events (`payment_failed` and `inventory_failed`) to cancel orders if necessary.

```js
// orderService.js
const express = require('express');
const mongoose = require('mongoose');
const rabbitmq = require('./rabbitmq');

const app = express();
app.use(express.json());

// Connect to MongoDB (order database)
mongoose.connect('mongodb://localhost:27017/orderdb', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('Order DB connected'))
.catch(err => console.error(err));

// Define Order schema and model
const orderSchema = new mongoose.Schema({
  productId: String,
  quantity: Number,
  status: { type: String, default: 'created' },
});
const Order = mongoose.model('Order', orderSchema);

// Endpoint to create a new order
app.post('/order', async (req, res) => {
  const { productId, quantity } = req.body;
  const order = new Order({ productId, quantity });
  await order.save();

  // Publish order_created event
  await rabbitmq.publish('order_created', { orderId: order._id, productId, quantity });
  res.send({ orderId: order._id, status: order.status });
});

// Listen for payment failure and cancel the order
rabbitmq.subscribe('payment_failed', async (message) => {
  const { orderId, reason } = message;
  await Order.findByIdAndUpdate(orderId, { status: 'cancelled' });
  console.log(`Order ${orderId} cancelled due to payment failure: ${reason}`);
});

// Listen for inventory failure and cancel the order
rabbitmq.subscribe('inventory_failed', async (message) => {
  const { orderId, reason } = message;
  await Order.findByIdAndUpdate(orderId, { status: 'cancelled' });
  console.log(`Order ${orderId} cancelled due to inventory failure: ${reason}`);
});

// Start the server
app.listen(3000, () => {
  console.log('Order Service listening on port 3000');
  rabbitmq.connect();
});
```

*Run this service (after installing dependencies such as `express`, `mongoose`, and `amqplib`) and use a tool like Postman to POST to `http://localhost:3000/order` with a JSON payload like:*

```json
{
  "productId": "product-1",
  "quantity": 2
}
```

---

## 2. Payment Service

This service listens for the `order_created` event, simulates payment processing (with a random chance to fail), stores the payment record, and publishes either a `payment_success` or `payment_failed` event.

```js
// paymentService.js
const mongoose = require('mongoose');
const rabbitmq = require('./rabbitmq');

// Connect to MongoDB (payment database)
mongoose.connect('mongodb://localhost:27017/paymentdb', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('Payment DB connected'))
.catch(err => console.error(err));

// Define Payment schema and model
const paymentSchema = new mongoose.Schema({
  orderId: String,
  status: String,
  amount: Number,
});
const Payment = mongoose.model('Payment', paymentSchema);

// Subscribe to order_created events
rabbitmq.subscribe('order_created', async (message) => {
  console.log('Payment Service received order_created:', message);
  const { orderId, productId, quantity } = message;

  // Simulate payment processing logic
  let paymentStatus = 'success';
  let amount = quantity * 10; // Example: cost per item is $10

  // Randomly simulate failure (20% chance)
  if (Math.random() < 0.2) {
    paymentStatus = 'failed';
  }

  const payment = new Payment({ orderId, status: paymentStatus, amount });
  await payment.save();

  if (paymentStatus === 'success') {
    await rabbitmq.publish('payment_success', { orderId, paymentId: payment._id, amount });
  } else {
    await rabbitmq.publish('payment_failed', { orderId, paymentId: payment._id, reason: 'Insufficient funds' });
  }
});

console.log('Payment Service waiting for order_created events...');
rabbitmq.connect();
```

---

## 3. Inventory Service

This service listens for the `payment_success` event, checks/reserves inventory, and then publishes an `inventory_reserved` event if stock is available or an `inventory_failed` event if not.

```js
// inventoryService.js
const mongoose = require('mongoose');
const rabbitmq = require('./rabbitmq');

// Connect to MongoDB (inventory database)
mongoose.connect('mongodb://localhost:27017/inventorydb', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('Inventory DB connected'))
.catch(err => console.error(err));

// Define Inventory schema and model
const inventorySchema = new mongoose.Schema({
  productId: String,
  stock: Number,
});
const Inventory = mongoose.model('Inventory', inventorySchema);

// Initialize inventory for demonstration (if not exists)
async function initInventory() {
  const existing = await Inventory.findOne({ productId: 'product-1' });
  if (!existing) {
    const inv = new Inventory({ productId: 'product-1', stock: 100 });
    await inv.save();
    console.log('Initialized inventory for product-1');
  }
}
initInventory();

// Subscribe to payment_success events
rabbitmq.subscribe('payment_success', async (message) => {
  console.log('Inventory Service received payment_success:', message);
  const { orderId } = message;

  // In a real scenario, productId and quantity would come from the order event.
  // For this demo, we assume productId 'product-1' and quantity 1.
  const productId = 'product-1';
  const quantity = 1;

  const product = await Inventory.findOne({ productId });
  if (product && product.stock >= quantity) {
    product.stock -= quantity;
    await product.save();
    await rabbitmq.publish('inventory_reserved', { orderId, productId, quantity });
  } else {
    await rabbitmq.publish('inventory_failed', { orderId, productId, reason: 'Out of stock' });
  }
});

console.log('Inventory Service waiting for payment_success events...');
rabbitmq.connect();
```

---

## 4. Delivery Service

This service listens for the `inventory_reserved` event, schedules the delivery (simulated by saving a record in MongoDB), and publishes a `delivery_scheduled` event.

```js
// deliveryService.js
const mongoose = require('mongoose');
const rabbitmq = require('./rabbitmq');

// Connect to MongoDB (delivery database)
mongoose.connect('mongodb://localhost:27017/deliverydb', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('Delivery DB connected'))
.catch(err => console.error(err));

// Define Delivery schema and model
const deliverySchema = new mongoose.Schema({
  orderId: String,
  status: { type: String, default: 'pending' },
  scheduledAt: Date,
});
const Delivery = mongoose.model('Delivery', deliverySchema);

// Subscribe to inventory_reserved events
rabbitmq.subscribe('inventory_reserved', async (message) => {
  console.log('Delivery Service received inventory_reserved:', message);
  const { orderId } = message;

  // Simulate scheduling a delivery 1 hour from now
  const scheduledAt = new Date(Date.now() + 3600 * 1000);
  const delivery = new Delivery({ orderId, scheduledAt });
  await delivery.save();

  // Optionally, publish a delivery_scheduled event
  await rabbitmq.publish('delivery_scheduled', { orderId, deliveryId: delivery._id, scheduledAt });
});

console.log('Delivery Service waiting for inventory_reserved events...');
rabbitmq.connect();
```

---

## How It Works Together

1. **Order Creation:**  
   When you POST an order (e.g., via Postman) to the Order Service, it saves the order and publishes an `order_created` event.

2. **Payment Processing:**  
   The Payment Service picks up the `order_created` event, processes the payment (with a 20% chance to simulate a failure), and then publishes either:
   - `payment_success` (if payment goes through), or
   - `payment_failed` (if payment fails).

3. **Inventory Reservation:**  
   - If payment is successful, the Inventory Service listens for the `payment_success` event, checks the inventory, and publishes:
     - `inventory_reserved` if the stock is available, or
     - `inventory_failed` if out of stock.
   - If inventory fails, the Order Service (which is listening for `inventory_failed`) cancels the order.

4. **Delivery Scheduling:**  
   When the Inventory Service publishes `inventory_reserved`, the Delivery Service schedules a delivery and optionally publishes a `delivery_scheduled` event.

5. **Compensation:**  
   If at any point a failure occurs (payment or inventory), the Order Service receives the corresponding event and marks the order as “cancelled.”

---

## Running the Example

1. **Set up RabbitMQ and MongoDB:**  
   Make sure both RabbitMQ (running on `amqp://localhost`) and MongoDB are up and running.

2. **Install Dependencies:**  
   In each service folder, run:
   ```bash
   npm install express mongoose amqplib
   ```
   *(For services that do not use Express, you can skip installing it.)*

3. **Start Each Service:**  
   Open separate terminal windows/tabs for each service and run:
   ```bash
   node orderService.js
   node paymentService.js
   node inventoryService.js
   node deliveryService.js
   ```

4. **Test the Flow:**  
   Use an HTTP client (like Postman or curl) to send a POST request to the Order Service at:
   ```
   http://localhost:3000/order
   ```
   with a JSON body:
   ```json
   {
     "productId": "product-1",
     "quantity": 2
   }
   ```
   Watch the console logs of each service to see how events are processed and how failures (if any) are handled.

---

This complete example demonstrates a simple, event-driven Saga pattern with compensation in a microservices environment using Node.js, RabbitMQ, and MongoDB. You can extend this basic setup by adding more detailed business logic, better error handling, retries, and persistence strategies for production-grade applications. Happy coding!