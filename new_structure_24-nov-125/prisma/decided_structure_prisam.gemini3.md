


Here is the complete, enterprise-grade code structure for the **Product Service**.

This setup follows your specific requests:
1.  **Monorepo-ready structure:** Root folder containing `services`.
2.  **No Service/Repository Layers:** Logic resides in the Controller (Fat Controller pattern).
3.  **Tech Stack:** Express, Prisma, Postgres, Kafka (KafkaJS), Zod.
4.  **Structure:** Strictly follows the provided folder tree, focusing only on the `product` module.

### Prerequisites
*   Node.js & npm/yarn installed.
*   PostgreSQL running.
*   Kafka/Zookeeper running.

---

### 1. Configuration Files (Root & Service Level)

File: services/products/package.json
```json
{
  "name": "product-service",
  "version": "1.0.0",
  "description": "Product microservice with Event Driven Architecture",
  "main": "dist/server.js",
  "scripts": {
    "dev": "ts-node-dev --respawn --transpile-only src/server.ts",
    "build": "tsc",
    "start": "node dist/server.js",
    "prisma:generate": "prisma generate --schema=./src/prisma/schema.prisma",
    "prisma:migrate": "prisma migrate dev --schema=./src/prisma/schema.prisma"
  },
  "dependencies": {
    "@prisma/client": "^5.0.0",
    "cors": "^2.8.5",
    "dotenv": "^16.3.1",
    "express": "^4.18.2",
    "helmet": "^7.0.0",
    "kafkajs": "^2.2.4",
    "morgan": "^1.10.0",
    "zod": "^3.21.4"
  },
  "devDependencies": {
    "@types/cors": "^2.8.13",
    "@types/express": "^4.17.17",
    "@types/morgan": "^1.9.4",
    "@types/node": "^20.4.2",
    "prisma": "^5.0.0",
    "ts-node-dev": "^2.0.0",
    "typescript": "^5.1.6"
  }
}
```

File: services/products/tsconfig.json
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "outDir": "./dist",
    "rootDir": "./src",
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
# Server
PORT=3001
NODE_ENV=development

# Database (PostgreSQL)
# Replace user, password, localhost, port, and db_name
DATABASE_URL="postgresql://postgres:password@localhost:5432/product_db?schema=public"

# Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=product-service
KAFKA_GROUP_ID=product-service-group
```

---

### 2. Prisma Setup (Database)

File: services/products/src/prisma/schema.prisma
```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

model Product {
  id          String   @id @default(uuid())
  name        String
  description String?
  price       Decimal
  sku         String   @unique
  stock       Int      @default(0)
  createdAt   DateTime @default(now())
  updatedAt   DateTime @updatedAt

  @@map("products")
}
```

---

### 3. Common Shared Logic (Infrastructure)

File: services/products/src/common/config.ts
```typescript
import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

// Validate Environment Variables on startup to fail fast
const envSchema = z.object({
  PORT: z.string().default('3001'),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  DATABASE_URL: z.string(),
  KAFKA_BROKERS: z.string(),
  KAFKA_CLIENT_ID: z.string(),
});

const envVars = envSchema.parse(process.env);

export const config = {
  port: parseInt(envVars.PORT, 10),
  env: envVars.NODE_ENV,
  dbUrl: envVars.DATABASE_URL,
  kafka: {
    brokers: envVars.KAFKA_BROKERS.split(','),
    clientId: envVars.KAFKA_CLIENT_ID,
  },
};
```

File: services/products/src/common/database/prisma.client.ts
```typescript
import { PrismaClient } from '@prisma/client';
import { config } from '../config';

// Singleton pattern for Prisma Client
const prisma = new PrismaClient({
  log: config.env === 'development' ? ['query', 'error', 'warn'] : ['error'],
});

export default prisma;
```

File: services/products/src/common/database/index.ts
```typescript
import prisma from './prisma.client';

export const connectDB = async () => {
  try {
    await prisma.$connect();
    console.log('✅ Database connected successfully');
  } catch (error) {
    console.error('❌ Database connection failed', error);
    process.exit(1);
  }
};
```

File: services/products/src/common/kafka/kafka.wrapper.ts
```typescript
import { Kafka, Producer, Consumer, logLevel } from 'kafkajs';
import { config } from '../config';

class KafkaWrapper {
  private kafka: Kafka;
  private producer: Producer;
  
  constructor() {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      logLevel: logLevel.ERROR,
    });
    this.producer = this.kafka.producer();
  }

  public async connect(): Promise<void> {
    try {
      await this.producer.connect();
      console.log('✅ Kafka Producer connected');
    } catch (error) {
      console.error('❌ Kafka connection error:', error);
    }
  }

  public async sendEvent(topic: string, message: any): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages: [{ value: JSON.stringify(message) }],
      });
      console.log(`📨 Event sent to topic [${topic}]`);
    } catch (error) {
      console.error(`❌ Error sending event to [${topic}]:`, error);
    }
  }

  public async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }
}

// Export singleton instance
export const kafkaWrapper = new KafkaWrapper();
```

File: services/products/src/common/middlewares/error-handler.ts
```typescript
import { Request, Response, NextFunction } from 'express';
import { ZodError } from 'zod';

export const errorHandler = (err: any, req: Request, res: Response, next: NextFunction) => {
  console.error('🔥 Error:', err);

  if (err instanceof ZodError) {
    return res.status(400).json({
      status: 'error',
      message: 'Validation Error',
      errors: err.errors,
    });
  }

  // Prisma Unique Constraint Error
  if (err.code === 'P2002') {
    return res.status(409).json({
      status: 'error',
      message: 'Duplicate field value entered',
    });
  }

  const statusCode = err.statusCode || 500;
  const message = err.message || 'Internal Server Error';

  res.status(statusCode).json({
    status: 'error',
    message,
  });
};
```

---

### 4. Module: Product (The Core Logic)

File: services/products/src/modules/product/product.schema.ts
```typescript
import { z } from 'zod';

// Create Product Schema
export const createProductSchema = z.object({
  name: z.string().min(3, "Name must be at least 3 chars"),
  description: z.string().optional(),
  price: z.number().positive("Price must be positive"),
  sku: z.string().min(3, "SKU is required"),
  stock: z.number().int().nonnegative().default(0),
});

// Update Product Schema
export const updateProductSchema = createProductSchema.partial();

// Types inferred from Zod
export type CreateProductDto = z.infer<typeof createProductSchema>;
export type UpdateProductDto = z.infer<typeof updateProductSchema>;
```

File: services/products/src/modules/product/events/product-created.producer.ts
```typescript
import { kafkaWrapper } from '../../../common/kafka/kafka.wrapper';

export const publishProductCreated = async (product: any) => {
  await kafkaWrapper.sendEvent('product.created', {
    productId: product.id,
    name: product.name,
    price: product.price,
    stock: product.stock,
    timestamp: new Date().toISOString(),
  });
};
```

File: services/products/src/modules/product/product.controller.ts
```typescript
import { Request, Response, NextFunction } from 'express';
import prisma from '../../common/database/prisma.client';
import { createProductSchema, updateProductSchema } from './product.schema';
import { publishProductCreated } from './events/product-created.producer';

// Create Product
export const createProduct = async (req: Request, res: Response, next: NextFunction) => {
  try {
    // 1. Validation (Zod)
    const validatedData = createProductSchema.parse(req.body);

    // 2. DB Interaction (Prisma) - No Repository Layer used
    const product = await prisma.product.create({
      data: validatedData,
    });

    // 3. Event Publishing (Kafka)
    await publishProductCreated(product);

    // 4. Response
    res.status(201).json({
      status: 'success',
      data: product,
    });
  } catch (err) {
    next(err); // Pass to global error handler
  }
};

// Get All Products
export const getProducts = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const products = await prisma.product.findMany({
      orderBy: { createdAt: 'desc' },
    });
    res.status(200).json({ status: 'success', data: products });
  } catch (err) {
    next(err);
  }
};

// Get Product By ID
export const getProductById = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;
    const product = await prisma.product.findUnique({ where: { id } });

    if (!product) {
      return res.status(404).json({ status: 'error', message: 'Product not found' });
    }

    res.status(200).json({ status: 'success', data: product });
  } catch (err) {
    next(err);
  }
};

// Update Product
export const updateProduct = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;
    const validatedData = updateProductSchema.parse(req.body);

    const product = await prisma.product.update({
      where: { id },
      data: validatedData,
    });

    // TODO: Publish product-updated event here

    res.status(200).json({ status: 'success', data: product });
  } catch (err) {
    next(err);
  }
};

// Delete Product
export const deleteProduct = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;
    await prisma.product.delete({ where: { id } });

    // TODO: Publish product-deleted event here

    res.status(204).send();
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
router.patch('/:id', ProductController.updateProduct);
router.delete('/:id', ProductController.deleteProduct);

export default router;
```

---

### 5. Application Entry Points

File: services/products/src/app.ts
```typescript
import express, { Application } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import morgan from 'morgan';
import productRoutes from './modules/product/product.routes';
import { errorHandler } from './common/middlewares/error-handler';

const app: Application = express();

// Global Middlewares
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
app.use(helmet());
app.use(morgan('dev'));

// Routes
app.use('/api/v1/products', productRoutes);

// Health Check
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'UP', service: 'product-service' });
});

// 404 Handler
app.use((req, res) => {
  res.status(404).json({ status: 'error', message: 'Route not found' });
});

// Global Error Handler
app.use(errorHandler);

export default app;
```

File: services/products/src/server.ts
```typescript
import app from './app';
import { config } from './common/config';
import { connectDB } from './common/database';
import { kafkaWrapper } from './common/kafka/kafka.wrapper';

const startServer = async () => {
  try {
    // 1. Connect to Database
    await connectDB();

    // 2. Connect to Kafka
    await kafkaWrapper.connect();

    // 3. Start Express Server
    const server = app.listen(config.port, () => {
      console.log(`🚀 Product Service running on port ${config.port} in ${config.env} mode`);
    });

    // Graceful Shutdown
    const shutdown = async () => {
      console.log('🛑 Shutting down server...');
      await kafkaWrapper.disconnect();
      server.close(() => {
        console.log('🛑 Server closed');
        process.exit(0);
      });
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);

  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
};

startServer();
```