Here is the complete, high-level enterprise-grade code structure for the **Products Service** following your strict architecture constraints (No Service Layer, No Repository Layer, "Fat Controller" pattern).

This setup includes robust error handling, Zod validation, structured logging (Pino), Kafka integration with graceful shutdown, and strong typing.

### Root Directory Structure

```text
/microservices-root
└── services
    └── products
        ├── src
        │   ├── common
        │   ├── modules
        │   ├── app.ts
        │   └── server.ts
        ├── .env
        ├── package.json
        └── tsconfig.json
```

---

### Configuration & Setup

File: services/products/package.json
```json
{
  "name": "products-service",
  "version": "1.0.0",
  "description": "Product microservice with Event Driven Architecture",
  "main": "dist/server.js",
  "scripts": {
    "dev": "ts-node-dev --respawn --transpile-only src/server.ts",
    "build": "tsc",
    "start": "node dist/server.js"
  },
  "dependencies": {
    "compression": "^1.7.4",
    "cors": "^2.8.5",
    "dotenv": "^16.3.1",
    "express": "^4.18.2",
    "helmet": "^7.1.0",
    "kafkajs": "^2.2.4",
    "mongoose": "^8.0.3",
    "pino": "^8.17.2",
    "pino-http": "^9.0.0",
    "zod": "^3.22.4"
  },
  "devDependencies": {
    "@types/compression": "^1.7.5",
    "@types/cors": "^2.8.17",
    "@types/express": "^4.17.21",
    "@types/node": "^20.10.5",
    "pino-pretty": "^10.3.1",
    "ts-node-dev": "^2.0.0",
    "typescript": "^5.3.3"
  }
}
```

File: services/products/tsconfig.json
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "commonjs",
    "rootDir": "./src",
    "outDir": "./dist",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules"]
}
```

File: services/products/.env
```env
NODE_ENV=development
PORT=3001
MONGO_URI=mongodb://localhost:27017/products_db
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=products-service
KAFKA_GROUP_ID=products-service-group
SERVICE_NAME=products-service
```

---

### Common Infrastructure

File: services/products/src/common/config.ts
```typescript
import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.string().transform(Number).default('3001'),
  MONGO_URI: z.string().min(1),
  KAFKA_BROKERS: z.string().min(1),
  KAFKA_CLIENT_ID: z.string().min(1),
  KAFKA_GROUP_ID: z.string().min(1),
  SERVICE_NAME: z.string().default('products-service'),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('❌ Invalid environment variables:', parsed.error.flatten().fieldErrors);
  process.exit(1);
}

export const config = parsed.data;
```

File: services/products/src/common/middlewares/logger.middleware.ts
```typescript
import pino from 'pino';
import pinoHttp from 'pino-http';
import { config } from '../config';

export const logger = pino({
  level: config.NODE_ENV === 'development' ? 'debug' : 'info',
  transport: config.NODE_ENV === 'development' 
    ? { target: 'pino-pretty', options: { colorize: true } } 
    : undefined,
  base: { service: config.SERVICE_NAME },
});

export const httpLogger = pinoHttp({ logger });
```

File: services/products/src/common/middlewares/error-handler.middleware.ts
```typescript
import { Request, Response, NextFunction } from 'express';
import { ZodError } from 'zod';
import { logger } from './logger.middleware';

export const errorHandler = (
  err: any, 
  req: Request, 
  res: Response, 
  next: NextFunction
) => {
  // Zod Validation Error
  if (err instanceof ZodError) {
    return res.status(400).json({
      status: 'error',
      message: 'Validation failed',
      errors: err.errors,
    });
  }

  // Mongoose Duplicate Key
  if (err.code === 11000) {
    return res.status(409).json({
      status: 'error',
      message: 'Duplicate record found',
      fields: Object.keys(err.keyPattern),
    });
  }

  // General Error
  logger.error(err);
  return res.status(500).json({
    status: 'error',
    message: config.NODE_ENV === 'production' ? 'Internal Server Error' : err.message,
  });
};
```

File: services/products/src/common/database/mongoose.connection.ts
```typescript
import mongoose from 'mongoose';
import { config } from '../config';
import { logger } from '../middlewares/logger.middleware';

export const connectDatabase = async () => {
  try {
    await mongoose.connect(config.MONGO_URI);
    logger.info('✅ MongoDB connected successfully');
  } catch (error) {
    logger.error('❌ MongoDB connection failed', error);
    process.exit(1);
  }
};

export const disconnectDatabase = async () => {
  await mongoose.disconnect();
  logger.info('🛑 MongoDB disconnected');
};
```

File: services/products/src/common/kafka/kafka.wrapper.ts
```typescript
import { Kafka, Producer, Consumer, logLevel } from 'kafkajs';
import { config } from '../config';
import { logger } from '../middlewares/logger.middleware';

class KafkaWrapper {
  private kafka: Kafka;
  private producer: Producer;
  private consumer: Consumer;
  private isConnected: boolean = false;

  constructor() {
    this.kafka = new Kafka({
      clientId: config.KAFKA_CLIENT_ID,
      brokers: config.KAFKA_BROKERS.split(','),
      logLevel: logLevel.ERROR, // Reduce noise
    });

    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: config.KAFKA_GROUP_ID });
  }

  public async connect(): Promise<void> {
    try {
      await this.producer.connect();
      await this.consumer.connect();
      this.isConnected = true;
      logger.info('✅ Kafka Producer & Consumer connected');
    } catch (error) {
      logger.error('❌ Kafka connection failed', error);
      process.exit(1);
    }
  }

  // Generic Send Method
  public async sendEvent(topic: string, message: any): Promise<void> {
    if (!this.isConnected) {
      logger.warn('Kafka not connected, skipping message');
      return;
    }
    try {
      await this.producer.send({
        topic,
        messages: [{ value: JSON.stringify(message) }],
      });
      logger.info(`📤 Event sent to topic: ${topic}`);
    } catch (error) {
      logger.error(`Failed to send event to ${topic}`, error);
    }
  }

  // Generic Subscribe Method
  public async subscribe(topic: string, handler: (message: any) => Promise<void>): Promise<void> {
    await this.consumer.subscribe({ topic, fromBeginning: false });
    
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        logger.debug(`- ${prefix} ${message.key}#${message.value}`);
        
        if (message.value) {
          try {
            const payload = JSON.parse(message.value.toString());
            await handler(payload);
          } catch (error) {
            logger.error(`Error processing Kafka message on topic ${topic}`, error);
          }
        }
      },
    });
  }

  public async disconnect(): Promise<void> {
    if (this.isConnected) {
      await this.producer.disconnect();
      await this.consumer.disconnect();
      this.isConnected = false;
      logger.info('🛑 Kafka disconnected');
    }
  }
}

export const kafkaWrapper = new KafkaWrapper();
```

---

### Module: Product (Core Logic)

File: services/products/src/modules/product/product.model.ts
```typescript
import mongoose, { Schema, Document } from 'mongoose';

export interface IProduct extends Document {
  name: string;
  price: number;
  description?: string;
  stock: number;
  createdAt: Date;
  updatedAt: Date;
}

const ProductSchema: Schema = new Schema(
  {
    name: { type: String, required: true, index: true },
    price: { type: Number, required: true },
    description: { type: String },
    stock: { type: Number, required: true, default: 0 },
  },
  { timestamps: true }
);

export const ProductModel = mongoose.model<IProduct>('Product', ProductSchema);
```

File: services/products/src/modules/product/product.schema.ts
```typescript
import { z } from 'zod';

// Input validation for Creating a product
export const createProductSchema = z.object({
  name: z.string().min(3, 'Name must be at least 3 chars'),
  price: z.number().positive('Price must be positive'),
  description: z.string().optional(),
  stock: z.number().int().nonnegative('Stock cannot be negative'),
});

// Input validation for Updating
export const updateProductSchema = createProductSchema.partial();

// Types inferred from schema
export type CreateProductInput = z.infer<typeof createProductSchema>;
export type UpdateProductInput = z.infer<typeof updateProductSchema>;
```

File: services/products/src/modules/product/events/product-created.producer.ts
```typescript
import { kafkaWrapper } from '../../../common/kafka/kafka.wrapper';
import { IProduct } from '../product.model';

export const publishProductCreated = async (product: IProduct) => {
  await kafkaWrapper.sendEvent('product.created', {
    productId: product._id,
    name: product.name,
    price: product.price,
    timestamp: new Date(),
  });
};
```

File: services/products/src/modules/product/events/product-updated.producer.ts
```typescript
import { kafkaWrapper } from '../../../common/kafka/kafka.wrapper';
import { IProduct } from '../product.model';

export const publishProductUpdated = async (product: IProduct) => {
  await kafkaWrapper.sendEvent('product.updated', {
    productId: product._id,
    updates: product.toObject(),
    timestamp: new Date(),
  });
};
```

File: services/products/src/modules/product/events/inventory-updated.consumer.ts
```typescript
import { logger } from '../../../common/middlewares/logger.middleware';
import { ProductModel } from '../product.model';

// Example: Another service says inventory changed, we update local product stock
// Note: Ideally validation and logic happen here
export const handleInventoryUpdated = async (payload: any) => {
  logger.info('📥 Received inventory.updated event', payload);
  
  const { productId, newStock } = payload;
  
  if (productId && newStock !== undefined) {
    await ProductModel.findByIdAndUpdate(productId, { stock: newStock });
    logger.info(`✅ Updated stock for product ${productId} to ${newStock}`);
  }
};
```

File: services/products/src/modules/product/product.controller.ts
```typescript
import { Request, Response, NextFunction } from 'express';
import { ProductModel } from './product.model';
import { createProductSchema, updateProductSchema } from './product.schema';
import { publishProductCreated } from './events/product-created.producer';
import { publishProductUpdated } from './events/product-updated.producer';

export const createProduct = async (req: Request, res: Response, next: NextFunction) => {
  try {
    // 1. Validation (Zod)
    const validatedData = createProductSchema.parse(req.body);

    // 2. DB Operation (Mongoose)
    const product = await ProductModel.create(validatedData);

    // 3. Kafka Event
    await publishProductCreated(product);

    // 4. Response
    return res.status(201).json({
      success: true,
      data: product,
    });
  } catch (err) {
    next(err); // Pass to global error handler
  }
};

export const getProducts = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const products = await ProductModel.find().sort({ createdAt: -1 }).limit(100);
    return res.status(200).json({
      success: true,
      count: products.length,
      data: products,
    });
  } catch (err) {
    next(err);
  }
};

export const getProductById = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const product = await ProductModel.findById(req.params.id);
    if (!product) {
      return res.status(404).json({ success: false, message: 'Product not found' });
    }
    return res.status(200).json({ success: true, data: product });
  } catch (err) {
    next(err);
  }
};

export const updateProduct = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const validatedData = updateProductSchema.parse(req.body);
    
    const product = await ProductModel.findByIdAndUpdate(
      req.params.id, 
      validatedData, 
      { new: true, runValidators: true }
    );

    if (!product) {
      return res.status(404).json({ success: false, message: 'Product not found' });
    }

    await publishProductUpdated(product);

    return res.status(200).json({ success: true, data: product });
  } catch (err) {
    next(err);
  }
};

export const deleteProduct = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const product = await ProductModel.findByIdAndDelete(req.params.id);
    if (!product) {
      return res.status(404).json({ success: false, message: 'Product not found' });
    }
    
    // Emitting deleted event directly here or via a specific producer function
    // await publishProductDeleted(product._id); 

    return res.status(200).json({ success: true, message: 'Product deleted' });
  } catch (err) {
    next(err);
  }
};
```

File: services/products/src/modules/product/product.routes.ts
```typescript
import { Router } from 'express';
import * as ProductController from './product.controller';

const router = Router();

router.post('/', ProductController.createProduct);
router.get('/', ProductController.getProducts);
router.get('/:id', ProductController.getProductById);
router.put('/:id', ProductController.updateProduct);
router.delete('/:id', ProductController.deleteProduct);

export default router;
```

---

### App & Server

File: services/products/src/app.ts
```typescript
import express, { Application } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import { httpLogger } from './common/middlewares/logger.middleware';
import { errorHandler } from './common/middlewares/error-handler.middleware';
import productRoutes from './modules/product/product.routes';

const app: Application = express();

// Security & Optimization Middlewares
app.use(helmet());
app.use(cors());
app.use(compression());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Logging
app.use(httpLogger);

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'UP', timestamp: new Date() });
});

// Module Routes
app.use('/api/v1/products', productRoutes);

// Global Error Handler
app.use(errorHandler);

export default app;
```

File: services/products/src/server.ts
```typescript
import app from './app';
import { config } from './common/config';
import { connectDatabase, disconnectDatabase } from './common/database/mongoose.connection';
import { kafkaWrapper } from './common/kafka/kafka.wrapper';
import { logger } from './common/middlewares/logger.middleware';
import { handleInventoryUpdated } from './modules/product/events/inventory-updated.consumer';

const startServer = async () => {
  try {
    // 1. Connect Database
    await connectDatabase();

    // 2. Connect Kafka
    await kafkaWrapper.connect();

    // 3. Subscribe to Consumers (e.g., Inventory changes affect Product stock)
    // Note: In real production, topics must exist before subscribing
    await kafkaWrapper.subscribe('inventory.updated', handleInventoryUpdated);

    // 4. Start Express Server
    const server = app.listen(config.PORT, () => {
      logger.info(`🚀 Product Service running on port ${config.PORT} in ${config.NODE_ENV} mode`);
    });

    // Graceful Shutdown Logic
    const shutdown = async () => {
      logger.info('🛑 Shutting down server...');
      server.close(async () => {
        await kafkaWrapper.disconnect();
        await disconnectDatabase();
        logger.info('👋 Goodbye!');
        process.exit(0);
      });
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);

  } catch (error) {
    logger.error('❌ Failed to start server', error);
    process.exit(1);
  }
};

startServer();
```