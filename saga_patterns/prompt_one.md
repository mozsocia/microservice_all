please teach me SAGA Microservices Architecture Patterns in node js in a beginner friendly manner

-complete example with failure handling,
-implement a simple example using Node.js and event-driven architecture. We'll use RabbitMQ for message queuing  and MongoDB for data storage.
- use below given Shared RabbitMQ Helper


```project-structure
saga-example/
├── services/
│   ├── order-service/
│   ├── payment-service/
│   ├── inventory-service/
│   └── delivery-service/
```

```
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


```
// shared/db/models.js
const mongoose = require('mongoose');

const OrderSchema = new mongoose.Schema({
    userId: String,
    items: [{
        productId: String,
        quantity: Number,
        price: Number
    }],
    totalAmount: Number,
    status: {
        type: String,
        enum: ['PENDING', 'PAYMENT_PENDING', 'PAID', 'FAILED', 'COMPLETED'],
        default: 'PENDING'
    },
    createdAt: { type: Date, default: Date.now }
});

const PaymentSchema = new mongoose.Schema({
    orderId: String,
    amount: Number,
    status: {
        type: String,
        enum: ['PENDING', 'COMPLETED', 'FAILED'],
        default: 'PENDING'
    },
    createdAt: { type: Date, default: Date.now }
});

const InventorySchema = new mongoose.Schema({
    productId: String,
    quantity: Number,
    reserved: Number
});

module.exports = {
    Order: mongoose.model('Order', OrderSchema),
    Payment: mongoose.model('Payment', PaymentSchema),
    Inventory: mongoose.model('Inventory', InventorySchema)
};

```