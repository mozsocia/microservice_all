I'll create a professional, enterprise-grade microservice architecture with Express.js, Prisma ORM, PostgreSQL, and Kafka. Let me provide you with the complete code structure.

## Root Project Structure

File: `docker-compose.yml`

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: microservices
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - microservices-network

  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    networks:
      - microservices-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - microservices-network

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
    networks:
      - microservices-network

volumes:
  postgres_data:

networks:
  microservices-network:
    driver: bridge
```

File: `package.json`

```json
{
  "name": "microservices-root",
  "version": "1.0.0",
  "description": "Microservices Architecture with Express, Prisma, Kafka",
  "private": true,
  "workspaces": [
    "services/*"
  ],
  "scripts": {
    "dev:products": "cd services/products && npm run dev",
    "build:products": "cd services/products && npm run build",
    "start:products": "cd services/products && npm run start",
    "docker:up": "docker-compose up -d",
    "docker:down": "docker-compose down",
    "docker:logs": "docker-compose logs -f"
  },
  "keywords": ["microservices", "kafka", "prisma", "express"],
  "author": "",
  "license": "ISC"
}
```

File: `.gitignore`

```gitignore
# Dependencies
node_modules/
*/node_modules/

# Environment files
.env
.env.local
.env.*.local
*/.env

# Build outputs
dist/
*/dist/
build/
*/build/

# Logs
logs/
*.log
npm-debug.log*
yarn-debug.log*
yarn-error.log*

# Database
*.sqlite
*.sqlite3
*.db

# IDE
.vscode/
.idea/
*.swp
*.swo
*~
.DS_Store

# Prisma
*/prisma/migrations/dev/
```

## Product Service

File: `services/products/.env`

```env
# Server Configuration
NODE_ENV=development
PORT=3001
SERVICE_NAME=product-service

# Database Configuration
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/product_db?schema=public"

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=product-service
KAFKA_CONSUMER_GROUP_ID=product-service-group

# JWT Configuration
JWT_SECRET=your-super-secret-jwt-key-change-in-production
JWT_EXPIRES_IN=7d

# API Configuration
API_VERSION=v1
API_PREFIX=/api/v1

# Logging
LOG_LEVEL=debug
LOG_FORMAT=json

# Rate Limiting
RATE_LIMIT_WINDOW_MS=900000
RATE_LIMIT_MAX_REQUESTS=100

# CORS
CORS_ORIGIN=http://localhost:3000,http://localhost:3001
```

File: `services/products/package.json`

```json
{
  "name": "product-service",
  "version": "1.0.0",
  "description": "Product Microservice",
  "main": "dist/server.js",
  "scripts": {
    "dev": "tsx watch src/server.ts",
    "build": "tsc",
    "start": "node dist/server.js",
    "prisma:generate": "prisma generate",
    "prisma:migrate:dev": "prisma migrate dev",
    "prisma:migrate:prod": "prisma migrate deploy",
    "prisma:seed": "tsx prisma/seed.ts",
    "prisma:studio": "prisma studio",
    "test": "jest",
    "test:watch": "jest --watch",
    "test:coverage": "jest --coverage",
    "lint": "eslint . --ext .ts",
    "lint:fix": "eslint . --ext .ts --fix"
  },
  "dependencies": {
    "@prisma/client": "^5.7.0",
    "compression": "^1.7.4",
    "cors": "^2.8.5",
    "dotenv": "^16.3.1",
    "express": "^4.18.2",
    "express-async-handler": "^1.2.0",
    "express-rate-limit": "^7.1.5",
    "helmet": "^7.1.0",
    "kafkajs": "^2.2.4",
    "morgan": "^1.10.0",
    "winston": "^3.11.0",
    "zod": "^3.22.4"
  },
  "devDependencies": {
    "@types/compression": "^1.7.5",
    "@types/cors": "^2.8.17",
    "@types/express": "^4.17.21",
    "@types/morgan": "^1.9.9",
    "@types/node": "^20.10.5",
    "@typescript-eslint/eslint-plugin": "^6.15.0",
    "@typescript-eslint/parser": "^6.15.0",
    "eslint": "^8.56.0",
    "jest": "^29.7.0",
    "prisma": "^5.7.0",
    "ts-jest": "^29.1.1",
    "tsx": "^4.7.0",
    "typescript": "^5.3.3"
  }
}
```

File: `services/products/tsconfig.json`

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "commonjs",
    "lib": ["ES2022"],
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "moduleResolution": "node",
    "allowSyntheticDefaultImports": true,
    "emitDecoratorMetadata": true,
    "experimentalDecorators": true,
    "typeRoots": ["./node_modules/@types", "./src/types"],
    "baseUrl": ".",
    "paths": {
      "@/*": ["src/*"],
      "@common/*": ["src/common/*"],
      "@modules/*": ["src/modules/*"],
      "@prisma/*": ["prisma/*"]
    }
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist", "**/*.spec.ts", "**/*.test.ts"]
}
```

File: `services/products/prisma/schema.prisma`

```prisma
// This is your Prisma schema file,
// learn more about it in the docs: https://pris.ly/d/prisma-schema

generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

model Product {
  id          String   @id @default(uuid())
  sku         String   @unique
  name        String
  description String?
  price       Decimal  @db.Decimal(10, 2)
  currency    String   @default("USD")
  quantity    Int      @default(0)
  status      ProductStatus @default(DRAFT)
  
  // Product attributes
  weight      Decimal? @db.Decimal(10, 3)
  dimensions  Json?    // { length: number, width: number, height: number }
  images      String[] // Array of image URLs
  
  // SEO
  slug        String   @unique
  metaTitle   String?
  metaDescription String?
  
  // Category and Brand (will be relations later)
  categoryId  String?
  brandId     String?
  
  // Tags (will be many-to-many later)
  tags        String[]
  
  // Audit fields
  createdAt   DateTime @default(now())
  updatedAt   DateTime @updatedAt
  deletedAt   DateTime? // Soft delete
  createdBy   String?
  updatedBy   String?
  
  // Indexes
  @@index([status])
  @@index([categoryId])
  @@index([brandId])
  @@index([slug])
  @@index([createdAt])
  @@map("products")
}

enum ProductStatus {
  DRAFT
  ACTIVE
  INACTIVE
  OUT_OF_STOCK
}
```

File: `services/products/prisma/seed.ts`

```typescript
import { PrismaClient, ProductStatus } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  console.log('🌱 Starting seed...');

  // Clean existing data
  await prisma.product.deleteMany();

  // Create sample products
  const products = await Promise.all([
    prisma.product.create({
      data: {
        sku: 'PROD-001',
        name: 'Premium Laptop',
        description: 'High-performance laptop for professionals',
        price: 1299.99,
        quantity: 50,
        status: ProductStatus.ACTIVE,
        weight: 2.5,
        dimensions: { length: 35, width: 25, height: 2 },
        images: [
          'https://example.com/laptop-1.jpg',
          'https://example.com/laptop-2.jpg'
        ],
        slug: 'premium-laptop',
        tags: ['electronics', 'computers', 'laptops'],
        metaTitle: 'Premium Laptop - Best Performance',
        metaDescription: 'Discover our premium laptop with cutting-edge performance'
      }
    }),
    prisma.product.create({
      data: {
        sku: 'PROD-002',
        name: 'Wireless Mouse',
        description: 'Ergonomic wireless mouse with precision tracking',
        price: 49.99,
        quantity: 200,
        status: ProductStatus.ACTIVE,
        weight: 0.15,
        dimensions: { length: 12, width: 7, height: 4 },
        images: ['https://example.com/mouse-1.jpg'],
        slug: 'wireless-mouse',
        tags: ['electronics', 'accessories', 'peripherals']
      }
    })
  ]);

  console.log(`✅ Seeded ${products.length} products`);
}

main()
  .catch((e) => {
    console.error('❌ Seed failed:', e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
```

File: `services/products/src/common/config.ts`

```typescript
import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  // Server
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.string().transform(Number).default('3001'),
  SERVICE_NAME: z.string().default('product-service'),
  
  // Database
  DATABASE_URL: z.string().url(),
  
  // Kafka
  KAFKA_BROKERS: z.string().transform(val => val.split(',')),
  KAFKA_CLIENT_ID: z.string().default('product-service'),
  KAFKA_CONSUMER_GROUP_ID: z.string().default('product-service-group'),
  
  // JWT
  JWT_SECRET: z.string().min(32),
  JWT_EXPIRES_IN: z.string().default('7d'),
  
  // API
  API_VERSION: z.string().default('v1'),
  API_PREFIX: z.string().default('/api/v1'),
  
  // Logging
  LOG_LEVEL: z.enum(['error', 'warn', 'info', 'debug']).default('info'),
  LOG_FORMAT: z.enum(['json', 'simple']).default('json'),
  
  // Rate Limiting
  RATE_LIMIT_WINDOW_MS: z.string().transform(Number).default('900000'),
  RATE_LIMIT_MAX_REQUESTS: z.string().transform(Number).default('100'),
  
  // CORS
  CORS_ORIGIN: z.string().transform(val => val.split(',')).default('*')
});

const parsedEnv = envSchema.safeParse(process.env);

if (!parsedEnv.success) {
  console.error('❌ Invalid environment variables:', parsedEnv.error.format());
  process.exit(1);
}

export const config = parsedEnv.data;

export const isDevelopment = config.NODE_ENV === 'development';
export const isProduction = config.NODE_ENV === 'production';
export const isTest = config.NODE_ENV === 'test';
```

File: `services/products/src/common/database/prisma.client.ts`

```typescript
import { PrismaClient } from '@prisma/client';
import { config, isDevelopment } from '../config';

declare global {
  // eslint-disable-next-line no-var
  var prisma: PrismaClient | undefined;
}

class PrismaService {
  private static instance: PrismaClient;

  private constructor() {}

  public static getInstance(): PrismaClient {
    if (!PrismaService.instance) {
      PrismaService.instance = new PrismaClient({
        log: isDevelopment 
          ? ['query', 'error', 'warn'] 
          : ['error'],
        errorFormat: isDevelopment ? 'pretty' : 'minimal',
      });

      // Middleware for soft delete
      PrismaService.instance.$use(async (params, next) => {
        if (params.model === 'Product') {
          if (params.action === 'findMany' || params.action === 'findFirst') {
            params.args = params.args || {};
            params.args.where = {
              ...params.args.where,
              deletedAt: null
            };
          }
        }
        return next(params);
      });
    }

    return PrismaService.instance;
  }

  public static async connect(): Promise<void> {
    try {
      await this.getInstance().$connect();
      console.log('✅ Database connected successfully');
    } catch (error) {
      console.error('❌ Database connection failed:', error);
      process.exit(1);
    }
  }

  public static async disconnect(): Promise<void> {
    await this.getInstance().$disconnect();
    console.log('Database disconnected');
  }
}

// Prevent multiple instances in development
const prisma = global.prisma || PrismaService.getInstance();
if (isDevelopment) global.prisma = prisma;

export default prisma;
export { PrismaService };
```

File: `services/products/src/common/database/index.ts`

```typescript
import prisma, { PrismaService } from './prisma.client';

export { prisma, PrismaService };

// Database health check
export const checkDatabaseHealth = async (): Promise<boolean> => {
  try {
    await prisma.$queryRaw`SELECT 1`;
    return true;
  } catch (error) {
    console.error('Database health check failed:', error);
    return false;
  }
};
```

File: `services/products/src/common/kafka/kafka.client.ts`

```typescript
import { Kafka, Producer, Consumer, EachMessagePayload, Admin } from 'kafkajs';
import { config } from '../config';
import winston from 'winston';

export class KafkaService {
  private kafka: Kafka;
  private producer: Producer | null = null;
  private consumers: Map<string, Consumer> = new Map();
  private admin: Admin | null = null;
  private logger: winston.Logger;

  private static instance: KafkaService;

  private constructor() {
    this.kafka = new Kafka({
      clientId: config.KAFKA_CLIENT_ID,
      brokers: config.KAFKA_BROKERS,
      retry: {
        initialRetryTime: 100,
        retries: 8
      },
      connectionTimeout: 30000,
      requestTimeout: 30000,
    });

    this.logger = winston.createLogger({
      level: config.LOG_LEVEL,
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
      ),
      defaultMeta: { service: config.SERVICE_NAME, component: 'kafka' },
      transports: [new winston.transports.Console()]
    });
  }

  public static getInstance(): KafkaService {
    if (!KafkaService.instance) {
      KafkaService.instance = new KafkaService();
    }
    return KafkaService.instance;
  }

  async connectProducer(): Promise<Producer> {
    if (!this.producer) {
      this.producer = this.kafka.producer({
        allowAutoTopicCreation: true,
        transactionTimeout: 30000
      });
      
      await this.producer.connect();
      this.logger.info('Kafka producer connected');
    }
    return this.producer;
  }

  async connectConsumer(groupId: string): Promise<Consumer> {
    if (!this.consumers.has(groupId)) {
      const consumer = this.kafka.consumer({ 
        groupId,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
        maxBytes: 10485760, // 10MB
      });
      
      await consumer.connect();
      this.consumers.set(groupId, consumer);
      this.logger.info(`Kafka consumer connected for group: ${groupId}`);
    }
    
    return this.consumers.get(groupId)!;
  }

  async connectAdmin(): Promise<Admin> {
    if (!this.admin) {
      this.admin = this.kafka.admin();
      await this.admin.connect();
      this.logger.info('Kafka admin connected');
    }
    return this.admin;
  }

  async createTopics(topics: string[]): Promise<void> {
    const admin = await this.connectAdmin();
    
    await admin.createTopics({
      topics: topics.map(topic => ({
        topic,
        numPartitions: 3,
        replicationFactor: 1,
      })),
    });
    
    this.logger.info(`Topics created: ${topics.join(', ')}`);
  }

  async produce(topic: string, message: any, key?: string): Promise<void> {
    const producer = await this.connectProducer();
    
    await producer.send({
      topic,
      messages: [{
        key: key || null,
        value: JSON.stringify(message),
        timestamp: Date.now().toString(),
        headers: {
          'service': config.SERVICE_NAME,
          'version': config.API_VERSION,
        }
      }],
    });
    
    this.logger.debug(`Message sent to topic ${topic}`, { topic, key, message });
  }

  async subscribe(
    topics: string[], 
    groupId: string, 
    handler: (payload: EachMessagePayload) => Promise<void>
  ): Promise<void> {
    const consumer = await this.connectConsumer(groupId);
    
    await consumer.subscribe({ 
      topics, 
      fromBeginning: false 
    });
    
    await consumer.run({
      eachMessage: async (payload) => {
        try {
          await handler(payload);
        } catch (error) {
          this.logger.error('Error processing message', { 
            error, 
            topic: payload.topic, 
            partition: payload.partition,
            offset: payload.message.offset 
          });
        }
      },
    });
    
    this.logger.info(`Subscribed to topics: ${topics.join(', ')}`);
  }

  async disconnect(): Promise<void> {
    if (this.producer) {
      await this.producer.disconnect();
      this.producer = null;
    }
    
    for (const [groupId, consumer] of this.consumers) {
      await consumer.disconnect();
    }
    this.consumers.clear();
    
    if (this.admin) {
      await this.admin.disconnect();
      this.admin = null;
    }
    
    this.logger.info('Kafka connections closed');
  }
}

export const kafkaService = KafkaService.getInstance();
```

File: `services/products/src/common/kafka/index.ts`

```typescript
export { KafkaService, kafkaService } from './kafka.client';

// Kafka Topics
export enum KafkaTopics {
  // Product Events
  PRODUCT_CREATED = 'product.created',
  PRODUCT_UPDATED = 'product.updated',
  PRODUCT_DELETED = 'product.deleted',
  PRODUCT_STOCK_UPDATED = 'product.stock.updated',
  
  // Inventory Events (consumed by product service)
  INVENTORY_UPDATED = 'inventory.updated',
  INVENTORY_LOW_STOCK = 'inventory.low.stock',
}

// Event Types
export interface KafkaEvent<T = any> {
  eventId: string;
  eventType: string;
  timestamp: string;
  source: string;
  version: string;
  data: T;
  metadata?: Record<string, any>;
}
```

File: `services/products/src/common/middlewares/error.middleware.ts`

```typescript
import { Request, Response, NextFunction } from 'express';
import { ZodError } from 'zod';
import { Prisma } from '@prisma/client';
import winston from 'winston';
import { config } from '../config';

const logger = winston.createLogger({
  level: config.LOG_LEVEL,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  defaultMeta: { service: config.SERVICE_NAME },
  transports: [new winston.transports.Console()]
});

export class AppError extends Error {
  statusCode: number;
  isOperational: boolean;

  constructor(message: string, statusCode: number = 500, isOperational: boolean = true) {
    super(message);
    this.statusCode = statusCode;
    this.isOperational = isOperational;
    Error.captureStackTrace(this, this.constructor);
  }
}

export const errorHandler = (
  err: Error,
  req: Request,
  res: Response,
  next: NextFunction
): void => {
  let statusCode = 500;
  let message = 'Internal Server Error';
  let errors: any = null;

  // Zod Validation Error
  if (err instanceof ZodError) {
    statusCode = 400;
    message = 'Validation Error';
    errors = err.errors.map(e => ({
      field: e.path.join('.'),
      message: e.message
    }));
  }
  // App Error
  else if (err instanceof AppError) {
    statusCode = err.statusCode;
    message = err.message;
  }
  // Prisma Errors
  else if (err instanceof Prisma.PrismaClientKnownRequestError) {
    if (err.code === 'P2002') {
      statusCode = 409;
      message = 'Duplicate entry found';
      errors = { fields: err.meta?.target };
    } else if (err.code === 'P2025') {
      statusCode = 404;
      message = 'Record not found';
    } else {
      message = 'Database operation failed';
    }
  }
  // Generic Error
  else if (err instanceof Error) {
    message = err.message;
  }

  // Log error
  logger.error({
    message: err.message,
    statusCode,
    stack: err.stack,
    path: req.path,
    method: req.method,
    ip: req.ip
  });

  // Send response
  res.status(statusCode).json({
    success: false,
    message,
    errors,
    ...(config.NODE_ENV === 'development' && { stack: err.stack })
  });
};

export const notFoundHandler = (req: Request, res: Response): void => {
  res.status(404).json({
    success: false,
    message: 'Resource not found',
    path: req.path
  });
};
```

File: `services/products/src/common/middlewares/logger.middleware.ts`

```typescript
import { Request, Response, NextFunction } from 'express';
import winston from 'winston';
import { config } from '../config';

const logger = winston.createLogger({
  level: config.LOG_LEVEL,
  format: config.LOG_FORMAT === 'json' 
    ? winston.format.json()
    : winston.format.simple(),
  defaultMeta: { service: config.SERVICE_NAME },
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          return `${timestamp} [${level}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}`;
        })
      )
    })
  ]
});

export const requestLogger = (req: Request, res: Response, next: NextFunction): void => {
  const start = Date.now();
  
  res.on('finish', () => {
    const duration = Date.now() - start;
    
    logger.info({
      method: req.method,
      url: req.originalUrl,
      status: res.statusCode,
      duration: `${duration}ms`,
      ip: req.ip,
      userAgent: req.get('user-agent')
    });
  });
  
  next();
};

export { logger };
```

File: `services/products/src/common/middlewares/validation.middleware.ts`

```typescript
import { Request, Response, NextFunction } from 'express';
import { AnyZodObject, ZodError } from 'zod';

export const validate = (schema: AnyZodObject) => {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      await schema.parseAsync({
        body: req.body,
        query: req.query,
        params: req.params,
      });
      next();
    } catch (error) {
      if (error instanceof ZodError) {
        return res.status(400).json({
          success: false,
          message: 'Validation failed',
          errors: error.errors.map(err => ({
            field: err.path.join('.'),
            message: err.message
          }))
        });
      }
      next(error);
    }
  };
};
```

File: `services/products/src/common/middlewares/index.ts`

```typescript
export { errorHandler, notFoundHandler, AppError } from './error.middleware';
export { requestLogger, logger } from './logger.middleware';
export { validate } from './validation.middleware';
```

File: `services/products/src/modules/product/product.types.ts`

```typescript
import { Product, ProductStatus, Prisma } from '@prisma/client';

// Base Product type from Prisma
export type { Product, ProductStatus };

// DTOs
export interface CreateProductDTO {
  sku: string;
  name: string;
  description?: string;
  price: number;
  currency?: string;
  quantity?: number;
  status?: ProductStatus;
  weight?: number;
  dimensions?: {
    length: number;
    width: number;
    height: number;
  };
  images?: string[];
  slug: string;
  metaTitle?: string;
  metaDescription?: string;
  categoryId?: string;
  brandId?: string;
  tags?: string[];
}

export interface UpdateProductDTO extends Partial<CreateProductDTO> {
  id: string;
}

export interface ProductQueryDTO {
  page?: number;
  limit?: number;
  sortBy?: string;
  sortOrder?: 'asc' | 'desc';
  search?: string;
  status?: ProductStatus;
  categoryId?: string;
  brandId?: string;
  minPrice?: number;
  maxPrice?: number;
  tags?: string[];
}

export interface ProductResponse {
  success: boolean;
  data?: Product | Product[];
  message?: string;
  pagination?: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

// Event Payloads
export interface ProductCreatedEvent {
  productId: string;
  sku: string;
  name: string;
  price: number;
  quantity: number;
  status: ProductStatus;
}

export interface ProductUpdatedEvent {
  productId: string;
  changes: Partial<Product>;
  previousValues: Partial<Product>;
}

export interface ProductDeletedEvent {
  productId: string;
  sku: string;
}

export interface ProductStockUpdatedEvent {
  productId: string;
  sku: string;
  previousQuantity: number;
  newQuantity: number;
  reason: 'sale' | 'restock' | 'adjustment' | 'return';
}
```

File: `services/products/src/modules/product/product.schema.ts`

```typescript
import { z } from 'zod';
import { ProductStatus } from '@prisma/client';

// Dimensions schema
const dimensionsSchema = z.object({
  length: z.number().positive('Length must be positive'),
  width: z.number().positive('Width must be positive'),
  height: z.number().positive('Height must be positive'),
});

// Create Product Schema
export const createProductSchema = z.object({
  body: z.object({
    sku: z.string()
      .min(3, 'SKU must be at least 3 characters')
      .max(50, 'SKU must not exceed 50 characters')
      .regex(/^[A-Z0-9-]+$/, 'SKU must contain only uppercase letters, numbers, and hyphens'),
    
    name: z.string()
      .min(2, 'Name must be at least 2 characters')
      .max(200, 'Name must not exceed 200 characters'),
    
    description: z.string()
      .max(2000, 'Description must not exceed 2000 characters')
      .optional(),
    
    price: z.number()
      .positive('Price must be positive')
      .multipleOf(0.01, 'Price must have at most 2 decimal places'),
    
    currency: z.string()
      .length(3, 'Currency must be 3 characters (ISO 4217)')
      .default('USD'),
    
    quantity: z.number()
      .int('Quantity must be an integer')
      .min(0, 'Quantity cannot be negative')
      .default(0),
    
    status: z.nativeEnum(ProductStatus)
      .default(ProductStatus.DRAFT),
    
    weight: z.number()
      .positive('Weight must be positive')
      .optional(),
    
    dimensions: dimensionsSchema.optional(),
    
    images: z.array(z.string().url('Invalid image URL'))
      .max(10, 'Maximum 10 images allowed')
      .default([]),
    
    slug: z.string()
      .min(2, 'Slug must be at least 2 characters')
      .max(200, 'Slug must not exceed 200 characters')
      .regex(/^[a-z0-9-]+$/, 'Slug must contain only lowercase letters, numbers, and hyphens'),
    
    metaTitle: z.string()
      .max(60, 'Meta title must not exceed 60 characters')
      .optional(),
    
    metaDescription: z.string()
      .max(160, 'Meta description must not exceed 160 characters')
      .optional(),
    
    categoryId: z.string().uuid('Invalid category ID').optional(),
    brandId: z.string().uuid('Invalid brand ID').optional(),
    
    tags: z.array(z.string())
      .max(20, 'Maximum 20 tags allowed')
      .default([]),
  })
});

// Update Product Schema
export const updateProductSchema = z.object({
  params: z.object({
    id: z.string().uuid('Invalid product ID'),
  }),
  body: z.object({
    sku: z.string()
      .min(3)
      .max(50)
      .regex(/^[A-Z0-9-]+$/)
      .optional(),
    
    name: z.string()
      .min(2)
      .max(200)
      .optional(),
    
    description: z.string()
      .max(2000)
      .optional(),
    
    price: z.number()
      .positive()
      .multipleOf(0.01)
      .optional(),
    
    currency: z.string()
      .length(3)
      .optional(),
    
    quantity: z.number()
      .int()
      .min(0)
      .optional(),
    
    status: z.nativeEnum(ProductStatus).optional(),
    
    weight: z.number()
      .positive()
      .optional(),
    
    dimensions: dimensionsSchema.optional(),
    
    images: z.array(z.string().url())
      .max(10)
      .optional(),
    
    slug: z.string()
      .min(2)
      .max(200)
      .regex(/^[a-z0-9-]+$/)
      .optional(),
    
    metaTitle: z.string()
      .max(60)
      .optional(),
    
    metaDescription: z.string()
      .max(160)
      .optional(),
    
    categoryId: z.string().uuid().nullable().optional(),
    brandId: z.string().uuid().nullable().optional(),
    
    tags: z.array(z.string())
      .max(20)
      .optional(),
  })
});

// Query Products Schema
export const queryProductsSchema = z.object({
  query: z.object({
    page: z.string().transform(Number).pipe(z.number().positive()).default('1'),
    limit: z.string().transform(Number).pipe(z.number().positive().max(100)).default('20'),
    sortBy: z.enum(['name', 'price', 'createdAt', 'updatedAt']).default('createdAt'),
    sortOrder: z.enum(['asc', 'desc']).default('desc'),
    search: z.string().optional(),
    status: z.nativeEnum(ProductStatus).optional(),
    categoryId: z.string().uuid().optional(),
    brandId: z.string().uuid().optional(),
    minPrice: z.string().transform(Number).pipe(z.number().positive()).optional(),
    maxPrice: z.string().transform(Number).pipe(z.number().positive()).optional(),
    tags: z.union([z.string(), z.array(z.string())]).transform(val => 
      Array.isArray(val) ? val : [val]
    ).optional(),
  })
});

// Get Single Product Schema
export const getProductSchema = z.object({
  params: z.object({
    id: z.string().uuid('Invalid product ID'),
  })
});

// Delete Product Schema
export const deleteProductSchema = z.object({
  params: z.object({
    id: z.string().uuid('Invalid product ID'),
  })
});

// Update Stock Schema
export const updateStockSchema = z.object({
  params: z.object({
    id: z.string().uuid('Invalid product ID'),
  }),
  body: z.object({
    quantity: z.number().int('Quantity must be an integer'),
    operation: z.enum(['add', 'subtract', 'set']),
    reason: z.enum(['sale', 'restock', 'adjustment', 'return']),
  })
});
```

File: `services/products/src/modules/product/product.repository.ts`

```typescript
import { Prisma, Product, ProductStatus } from '@prisma/client';
import { prisma } from '../../common/database';
import { CreateProductDTO, UpdateProductDTO, ProductQueryDTO } from './product.types';

export class ProductRepository {
  // Create product
  static async create(data: CreateProductDTO): Promise<Product> {
    return prisma.product.create({
      data: {
        ...data,
        price: new Prisma.Decimal(data.price),
        weight: data.weight ? new Prisma.Decimal(data.weight) : undefined,
      },
    });
  }

  // Find products with pagination and filters
  static async findMany(query: ProductQueryDTO) {
    const {
      page = 1,
      limit = 20,
      sortBy = 'createdAt',
      sortOrder = 'desc',
      search,
      status,
      categoryId,
      brandId,
      minPrice,
      maxPrice,
      tags
    } = query;

    const skip = (page - 1) * limit;

    const where: Prisma.ProductWhereInput = {
      AND: [
        search ? {
          OR: [
            { name: { contains: search, mode: 'insensitive' } },
            { description: { contains: search, mode: 'insensitive' } },
            { sku: { contains: search, mode: 'insensitive' } },
          ]
        } : {},
        status ? { status } : {},
        categoryId ? { categoryId } : {},
        brandId ? { brandId } : {},
        minPrice ? { price: { gte: new Prisma.Decimal(minPrice) } } : {},
        maxPrice ? { price: { lte: new Prisma.Decimal(maxPrice) } } : {},
        tags && tags.length > 0 ? { tags: { hasSome: tags } } : {},
      ]
    };

    const [products, total] = await Promise.all([
      prisma.product.findMany({
        where,
        skip,
        take: limit,
        orderBy: { [sortBy]: sortOrder },
      }),
      prisma.product.count({ where }),
    ]);

    return {
      products,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    };
  }

  // Find single product by ID
  static async findById(id: string): Promise<Product | null> {
    return prisma.product.findUnique({
      where: { id },
    });
  }

  // Find single product by SKU
  static async findBySku(sku: string): Promise<Product | null> {
    return prisma.product.findUnique({
      where: { sku },
    });
  }

  // Find single product by slug
  static async findBySlug(slug: string): Promise<Product | null> {
    return prisma.product.findUnique({
      where: { slug },
    });
  }

  // Update product
  static async update(id: string, data: Partial<UpdateProductDTO>): Promise<Product> {
    const updateData: any = { ...data };
    
    if (data.price !== undefined) {
      updateData.price = new Prisma.Decimal(data.price);
    }
    
    if (data.weight !== undefined) {
      updateData.weight = new Prisma.Decimal(data.weight);
    }

    return prisma.product.update({
      where: { id },
      data: updateData,
    });
  }

  // Update stock
  static async updateStock(
    id: string, 
    quantity: number, 
    operation: 'add' | 'subtract' | 'set'
  ): Promise<Product> {
    const product = await this.findById(id);
    if (!product) {
      throw new Error('Product not found');
    }

    let newQuantity: number;
    
    switch (operation) {
      case 'add':
        newQuantity = product.quantity + quantity;
        break;
      case 'subtract':
        newQuantity = Math.max(0, product.quantity - quantity);
        break;
      case 'set':
        newQuantity = Math.max(0, quantity);
        break;
      default:
        throw new Error('Invalid operation');
    }

    const status = newQuantity === 0 ? ProductStatus.OUT_OF_STOCK : product.status;

    return prisma.product.update({
      where: { id },
      data: { 
        quantity: newQuantity,
        status 
      },
    });
  }

  // Soft delete
  static async softDelete(id: string): Promise<Product> {
    return prisma.product.update({
      where: { id },
      data: { 
        deletedAt: new Date(),
        status: ProductStatus.INACTIVE 
      },
    });
  }

  // Hard delete (use with caution)
  static async hardDelete(id: string): Promise<Product> {
    return prisma.product.delete({
      where: { id },
    });
  }

  // Check if SKU exists
  static async skuExists(sku: string, excludeId?: string): Promise<boolean> {
    const product = await prisma.product.findUnique({
      where: { sku },
      select: { id: true },
    });
    
    if (!product) return false;
    if (excludeId && product.id === excludeId) return false;
    return true;
  }

  // Check if slug exists
  static async slugExists(slug: string, excludeId?: string): Promise<boolean> {
    const product = await prisma.product.findUnique({
      where: { slug },
      select: { id: true },
    });
    
    if (!product) return false;
    if (excludeId && product.id === excludeId) return false;
    return true;
  }
}
```

File: `services/products/src/modules/product/events/product-created.producer.ts`

```typescript
import { Product } from '@prisma/client';
import { kafkaService } from '../../../common/kafka';
import { KafkaTopics, KafkaEvent } from '../../../common/kafka';
import { ProductCreatedEvent } from '../product.types';
import { config } from '../../../common/config';
import { v4 as uuidv4 } from 'uuid';

export class ProductCreatedProducer {
  static async publish(product: Product): Promise<void> {
    const eventPayload: ProductCreatedEvent = {
      productId: product.id,
      sku: product.sku,
      name: product.name,
      price: product.price.toNumber(),
      quantity: product.quantity,
      status: product.status,
    };

    const event: KafkaEvent<ProductCreatedEvent> = {
      eventId: uuidv4(),
      eventType: KafkaTopics.PRODUCT_CREATED,
      timestamp: new Date().toISOString(),
      source: config.SERVICE_NAME,
      version: '1.0',
      data: eventPayload,
      metadata: {
        correlationId: uuidv4(),
        userId: product.createdBy || 'system',
      }
    };

    await kafkaService.produce(
      KafkaTopics.PRODUCT_CREATED,
      event,
      product.id
    );
  }
}
```

File: `services/products/src/modules/product/events/product-updated.producer.ts`

```typescript
import { Product } from '@prisma/client';
import { kafkaService } from '../../../common/kafka';
import { KafkaTopics, KafkaEvent } from '../../../common/kafka';
import { ProductUpdatedEvent } from '../product.types';
import { config } from '../../../common/config';
import { v4 as uuidv4 } from 'uuid';

export class ProductUpdatedProducer {
  static async publish(
    product: Product, 
    previousProduct: Product
  ): Promise<void> {
    const changes: Partial<Product> = {};
    const previousValues: Partial<Product> = {};

    // Detect changes
    Object.keys(product).forEach(key => {
      if (JSON.stringify(product[key as keyof Product]) !== 
          JSON.stringify(previousProduct[key as keyof Product])) {
        changes[key as keyof Product] = product[key as keyof Product] as any;
        previousValues[key as keyof Product] = previousProduct[key as keyof Product] as any;
      }
    });

    const eventPayload: ProductUpdatedEvent = {
      productId: product.id,
      changes,
      previousValues,
    };

    const event: KafkaEvent<ProductUpdatedEvent> = {
      eventId: uuidv4(),
      eventType: KafkaTopics.PRODUCT_UPDATED,
      timestamp: new Date().toISOString(),
      source: config.SERVICE_NAME,
      version: '1.0',
      data: eventPayload,
      metadata: {
        correlationId: uuidv4(),
        userId: product.updatedBy || 'system',
      }
    };

    await kafkaService.produce(
      KafkaTopics.PRODUCT_UPDATED,
      event,
      product.id
    );
  }
}
```

File: `services/products/src/modules/product/events/product-deleted.producer.ts`

```typescript
import { Product } from '@prisma/client';
import { kafkaService } from '../../../common/kafka';
import { KafkaTopics, KafkaEvent } from '../../../common/kafka';
import { ProductDeletedEvent } from '../product.types';
import { config } from '../../../common/config';
import { v4 as uuidv4 } from 'uuid';

export class ProductDeletedProducer {
  static async publish(product: Product): Promise<void> {
    const eventPayload: ProductDeletedEvent = {
      productId: product.id,
      sku: product.sku,
    };

    const event: KafkaEvent<ProductDeletedEvent> = {
      eventId: uuidv4(),
      eventType: KafkaTopics.PRODUCT_DELETED,
      timestamp: new Date().toISOString(),
      source: config.SERVICE_NAME,
      version: '1.0',
      data: eventPayload,
      metadata: {
        correlationId: uuidv4(),
        userId: product.updatedBy || 'system',
      }
    };

    await kafkaService.produce(
      KafkaTopics.PRODUCT_DELETED,
      event,
      product.id
    );
  }
}
```

File: `services/products/src/modules/product/events/inventory-updated.consumer.ts`

```typescript
import { EachMessagePayload } from 'kafkajs';
import { kafkaService } from '../../../common/kafka';
import { KafkaTopics } from '../../../common/kafka';
import { ProductRepository } from '../product.repository';
import { logger } from '../../../common/middlewares';
import { config } from '../../../common/config';

interface InventoryUpdatedPayload {
  productId: string;
  sku: string;
  quantity: number;
  operation: 'set' | 'add' | 'subtract';
  reason: string;
}

export class InventoryUpdatedConsumer {
  static async start(): Promise<void> {
    await kafkaService.subscribe(
      [KafkaTopics.INVENTORY_UPDATED],
      config.KAFKA_CONSUMER_GROUP_ID,
      this.handleMessage
    );

    logger.info('Inventory updated consumer started');
  }

  static async handleMessage(payload: EachMessagePayload): Promise<void> {
    try {
      const { topic, partition, message } = payload;
      
      if (!message.value) {
        logger.warn('Received empty message');
        return;
      }

      const event = JSON.parse(message.value.toString());
      const data: InventoryUpdatedPayload = event.data;

      logger.info('Processing inventory update', {
        topic,
        partition,
        offset: message.offset,
        productId: data.productId,
      });

      // Update product stock based on inventory changes
      await ProductRepository.updateStock(
        data.productId,
        data.quantity,
        data.operation
      );

      logger.info('Product stock updated successfully', {
        productId: data.productId,
        quantity: data.quantity,
        operation: data.operation,
      });
    } catch (error) {
      logger.error('Error processing inventory update', { error });
      throw error;
    }
  }
}
```

File: `services/products/src/modules/product/product.controller.ts`

```typescript
import { Request, Response } from 'express';
import { ProductRepository } from './product.repository';
import { 
  createProductSchema, 
  updateProductSchema, 
  queryProductsSchema,
  updateStockSchema 
} from './product.schema';
import { AppError } from '../../common/middlewares';
import { ProductCreatedProducer } from './events/product-created.producer';
import { ProductUpdatedProducer } from './events/product-updated.producer';
import { ProductDeletedProducer } from './events/product-deleted.producer';
import { logger } from '../../common/middlewares';

export class ProductController {
  // Create new product
  static async create(req: Request, res: Response): Promise<void> {
    try {
      // Validate request body
      const validatedData = createProductSchema.parse({ body: req.body });
      const productData = validatedData.body;

      // Check if SKU already exists
      const skuExists = await ProductRepository.skuExists(productData.sku);
      if (skuExists) {
        throw new AppError('SKU already exists', 409);
      }

      // Check if slug already exists
      const slugExists = await ProductRepository.slugExists(productData.slug);
      if (slugExists) {
        throw new AppError('Slug already exists', 409);
      }

      // Create product in database
      const product = await ProductRepository.create(productData);

      // Publish product created event
      await ProductCreatedProducer.publish(product);

      logger.info('Product created successfully', { productId: product.id });

      res.status(201).json({
        success: true,
        message: 'Product created successfully',
        data: product,
      });
    } catch (error) {
      if (error instanceof AppError) {
        res.status(error.statusCode).json({
          success: false,
          message: error.message,
        });
      } else {
        throw error;
      }
    }
  }

  // Get all products with filters
  static async getAll(req: Request, res: Response): Promise<void> {
    try {
      // Validate and parse query parameters
      const validatedQuery = queryProductsSchema.parse({ query: req.query });
      const queryParams = validatedQuery.query;

      // Fetch products from database
      const result = await ProductRepository.findMany(queryParams);

      res.status(200).json({
        success: true,
        data: result.products,
        pagination: result.pagination,
      });
    } catch (error) {
      throw error;
    }
  }

  // Get single product by ID
  static async getById(req: Request, res: Response): Promise<void> {
    try {
      const { id } = req.params;

      const product = await ProductRepository.findById(id);
      if (!product) {
        throw new AppError('Product not found', 404);
      }

      res.status(200).json({
        success: true,
        data: product,
      });
    } catch (error) {
      if (error instanceof AppError) {
        res.status(error.statusCode).json({
          success: false,
          message: error.message,
        });
      } else {
        throw error;
      }
    }
  }

  // Get product by slug
  static async getBySlug(req: Request, res: Response): Promise<void> {
    try {
      const { slug } = req.params;

      const product = await ProductRepository.findBySlug(slug);
      if (!product) {
        throw new AppError('Product not found', 404);
      }

      res.status(200).json({
        success: true,
        data: product,
      });
    } catch (error) {
      if (error instanceof AppError) {
        res.status(error.statusCode).json({
          success: false,
          message: error.message,
        });
      } else {
        throw error;
      }
    }
  }

  // Update product
  static async update(req: Request, res: Response): Promise<void> {
    try {
      // Validate request
      const validatedData = updateProductSchema.parse({ 
        params: req.params, 
        body: req.body 
      });
      const { id } = validatedData.params;
      const updateData = validatedData.body;

      // Check if product exists
      const existingProduct = await ProductRepository.findById(id);
      if (!existingProduct) {
        throw new AppError('Product not found', 404);
      }

      // Check SKU uniqueness if updating
      if (updateData.sku && updateData.sku !== existingProduct.sku) {
        const skuExists = await ProductRepository.skuExists(updateData.sku, id);
        if (skuExists) {
          throw new AppError('SKU already exists', 409);
        }
      }

      // Check slug uniqueness if updating
      if (updateData.slug && updateData.slug !== existingProduct.slug) {
        const slugExists = await ProductRepository.slugExists(updateData.slug, id);
        if (slugExists) {
          throw new AppError('Slug already exists', 409);
        }
      }

      // Update product in database
      const updatedProduct = await ProductRepository.update(id, updateData);

      // Publish product updated event
      await ProductUpdatedProducer.publish(updatedProduct, existingProduct);

      logger.info('Product updated successfully', { productId: id });

      res.status(200).json({
        success: true,
        message: 'Product updated successfully',
        data: updatedProduct,
      });
    } catch (error) {
      if (error instanceof AppError) {
        res.status(error.statusCode).json({
          success: false,
          message: error.message,
        });
      } else {
        throw error;
      }
    }
  }

  // Update product stock
  static async updateStock(req: Request, res: Response): Promise<void> {
    try {
      // Validate request
      const validatedData = updateStockSchema.parse({ 
        params: req.params, 
        body: req.body 
      });
      const { id } = validatedData.params;
      const { quantity, operation, reason } = validatedData.body;

      // Check if product exists
      const existingProduct = await ProductRepository.findById(id);
      if (!existingProduct) {
        throw new AppError('Product not found', 404);
      }

      // Update stock
      const updatedProduct = await ProductRepository.updateStock(id, quantity, operation);

      // Publish stock updated event
      // This would trigger inventory service events

      logger.info('Product stock updated', { 
        productId: id, 
        quantity, 
        operation, 
        reason 
      });

      res.status(200).json({
        success: true,
        message: 'Stock updated successfully',
        data: updatedProduct,
      });
    } catch (error) {
      if (error instanceof AppError) {
        res.status(error.statusCode).json({
          success: false,
          message: error.message,
        });
      } else {
        throw error;
      }
    }
  }

  // Delete product (soft delete)
  static async delete(req: Request, res: Response): Promise<void> {
    try {
      const { id } = req.params;

      // Check if product exists
      const product = await ProductRepository.findById(id);
      if (!product) {
        throw new AppError('Product not found', 404);
      }

      // Soft delete product
      const deletedProduct = await ProductRepository.softDelete(id);

      // Publish product deleted event
      await ProductDeletedProducer.publish(deletedProduct);

      logger.info('Product deleted successfully', { productId: id });

      res.status(200).json({
        success: true,
        message: 'Product deleted successfully',
      });
    } catch (error) {
      if (error instanceof AppError) {
        res.status(error.statusCode).json({
          success: false,
          message: error.message,
        });
      } else {
        throw error;
      }
    }
  }
}
```

File: `services/products/src/modules/product/product.routes.ts`

```typescript
import { Router } from 'express';
import asyncHandler from 'express-async-handler';
import { ProductController } from './product.controller';
import { validate } from '../../common/middlewares';
import {
  createProductSchema,
  updateProductSchema,
  queryProductsSchema,
  getProductSchema,
  deleteProductSchema,
  updateStockSchema
} from './product.schema';

const router = Router();

// Product routes
router.post(
  '/',
  validate(createProductSchema),
  asyncHandler(ProductController.create)
);

router.get(
  '/',
  validate(queryProductsSchema),
  asyncHandler(ProductController.getAll)
);

router.get(
  '/:id',
  validate(getProductSchema),
  asyncHandler(ProductController.getById)
);

router.get(
  '/slug/:slug',
  asyncHandler(ProductController.getBySlug)
);

router.put(
  '/:id',
  validate(updateProductSchema),
  asyncHandler(ProductController.update)
);

router.patch(
  '/:id/stock',
  validate(updateStockSchema),
  asyncHandler(ProductController.updateStock)
);

router.delete(
  '/:id',
  validate(deleteProductSchema),
  asyncHandler(ProductController.delete)
);

export default router;
```

File: `services/products/src/app.ts`

```typescript
import express, { Application } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import morgan from 'morgan';
import rateLimit from 'express-rate-limit';
import { config } from './common/config';
import { errorHandler, notFoundHandler, requestLogger } from './common/middlewares';

// Import routes
import productRoutes from './modules/product/product.routes';

class App {
  public app: Application;

  constructor() {
    this.app = express();
    this.initializeMiddlewares();
    this.initializeRoutes();
    this.initializeErrorHandling();
  }

  private initializeMiddlewares(): void {
    // Security middlewares
    this.app.use(helmet());
    this.app.use(cors({
      origin: config.CORS_ORIGIN,
      credentials: true,
    }));

    // Rate limiting
    const limiter = rateLimit({
      windowMs: config.RATE_LIMIT_WINDOW_MS,
      max: config.RATE_LIMIT_MAX_REQUESTS,
      message: 'Too many requests from this IP, please try again later.',
      standardHeaders: true,
      legacyHeaders: false,
    });
    this.app.use(limiter);

    // Body parsing
    this.app.use(express.json({ limit: '10mb' }));
    this.app.use(express.urlencoded({ extended: true, limit: '10mb' }));

    // Compression
    this.app.use(compression());

    // Logging
    if (config.NODE_ENV !== 'test') {
      this.app.use(morgan('combined'));
      this.app.use(requestLogger);
    }

    // Health check endpoint
    this.app.get('/health', (req, res) => {
      res.status(200).json({
        status: 'healthy',
        service: config.SERVICE_NAME,
        version: config.API_VERSION,
        timestamp: new Date().toISOString(),
      });
    });

    // Ready check endpoint
    this.app.get('/ready', async (req, res) => {
      try {
        // Check database connection
        const { checkDatabaseHealth } = await import('./common/database');
        const dbHealthy = await checkDatabaseHealth();
        
        if (!dbHealthy) {
          return res.status(503).json({
            status: 'not ready',
            service: config.SERVICE_NAME,
            reason: 'Database connection failed',
          });
        }

        res.status(200).json({
          status: 'ready',
          service: config.SERVICE_NAME,
        });
      } catch (error) {
        res.status(503).json({
          status: 'not ready',
          service: config.SERVICE_NAME,
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      }
    });
  }

  private initializeRoutes(): void {
    // API routes
    this.app.use(`${config.API_PREFIX}/products`, productRoutes);
    
    // Future module routes would go here:
    // this.app.use(`${config.API_PREFIX}/categories`, categoryRoutes);
    // this.app.use(`${config.API_PREFIX}/brands`, brandRoutes);
    // this.app.use(`${config.API_PREFIX}/tags`, tagRoutes);
  }

  private initializeErrorHandling(): void {
    // 404 handler
    this.app.use(notFoundHandler);
    
    // Global error handler
    this.app.use(errorHandler);
  }
}

export default new App().app;
```

File: `services/products/src/server.ts`

```typescript
import app from './app';
import { config, isDevelopment } from './common/config';
import { PrismaService } from './common/database';
import { kafkaService } from './common/kafka';
import { KafkaTopics } from './common/kafka';
import { InventoryUpdatedConsumer } from './modules/product/events/inventory-updated.consumer';
import { logger } from './common/middlewares';

class Server {
  private port: number;

  constructor() {
    this.port = config.PORT;
  }

  private async initializeDatabase(): Promise<void> {
    await PrismaService.connect();
  }

  private async initializeKafka(): Promise<void> {
    try {
      // Connect to Kafka
      await kafkaService.connectProducer();
      
      // Create topics if they don't exist
      await kafkaService.createTopics([
        KafkaTopics.PRODUCT_CREATED,
        KafkaTopics.PRODUCT_UPDATED,
        KafkaTopics.PRODUCT_DELETED,
        KafkaTopics.PRODUCT_STOCK_UPDATED,
      ]);

      // Start consumers
      await InventoryUpdatedConsumer.start();
      
      logger.info('Kafka initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Kafka', { error });
      if (!isDevelopment) {
        throw error;
      }
    }
  }

  private setupGracefulShutdown(): void {
    const gracefulShutdown = async (signal: string) => {
      logger.info(`${signal} received. Starting graceful shutdown...`);
      
      try {
        // Close server
        await new Promise<void>((resolve) => {
          const server = app.listen();
          server.close(() => resolve());
        });
        
        // Disconnect from Kafka
        await kafkaService.disconnect();
        
        // Disconnect from database
        await PrismaService.disconnect();
        
        logger.info('Graceful shutdown completed');
        process.exit(0);
      } catch (error) {
        logger.error('Error during graceful shutdown', { error });
        process.exit(1);
      }
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
    process.on('unhandledRejection', (reason, promise) => {
      logger.error('Unhandled Rejection at:', { promise, reason });
    });
    process.on('uncaughtException', (error) => {
      logger.error('Uncaught Exception:', { error });
      process.exit(1);
    });
  }

  public async start(): Promise<void> {
    try {
      // Initialize database
      await this.initializeDatabase();

      // Initialize Kafka
      await this.initializeKafka();

      // Setup graceful shutdown
      this.setupGracefulShutdown();

      // Start Express server
      app.listen(this.port, () => {
        logger.info(`🚀 ${config.SERVICE_NAME} is running on port ${this.port}`);
        logger.info(`📍 Environment: ${config.NODE_ENV}`);
        logger.info(`🔗 API Prefix: ${config.API_PREFIX}`);
      });
    } catch (error) {
      logger.error('Failed to start server', { error });
      process.exit(1);
    }
  }
}

// Start the server
const server = new Server();
server.start();
```

File: `services/products/.eslintrc.json`

```json
{
  "parser": "@typescript-eslint/parser",
  "parserOptions": {
    "project": "./tsconfig.json",
    "sourceType": "module"
  },
  "plugins": ["@typescript-eslint"],
  "extends": [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended"
  ],
  "rules": {
    "@typescript-eslint/no-explicit-any": "warn",
    "@typescript-eslint/explicit-function-return-type": "off",
    "@typescript-eslint/explicit-module-boundary-types": "off",
    "@typescript-eslint/no-unused-vars": ["error", { "argsIgnorePattern": "^_" }],
    "no-console": "warn"
  },
  "env": {
    "node": true,
    "es2022": true
  }
}
```

File: `services/products/jest.config.js`

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src'],
  testMatch: ['**/__tests__/**/*.ts', '**/?(*.)+(spec|test).ts'],
  transform: {
    '^.+\\.ts$': 'ts-jest',
  },
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.d.ts',
    '!src/**/*.spec.ts',
    '!src/**/*.test.ts',
    '!src/**/__tests__/**',
    '!src/server.ts',
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
};
```

## Setup Instructions

1. **Install dependencies:**
```bash
# Root directory
npm install

# Product service
cd services/products
npm install
```

2. **Setup database:**
```bash
# Start PostgreSQL and Kafka using Docker
docker-compose up -d

# Run Prisma migrations
cd services/products
npx prisma migrate dev --name init

# Seed the database
npm run prisma:seed
```

3. **Start the service:**
```bash
# Development mode
npm run dev

# Production build
npm run build
npm start
```

This is a professional, enterprise-grade microservice architecture with:
- ✅ Complete error handling and validation
- ✅ Kafka event-driven architecture
- ✅ Prisma ORM with PostgreSQL
- ✅ TypeScript with strict typing
- ✅ Comprehensive logging
- ✅ Health checks and readiness probes
- ✅ Graceful shutdown handling
- ✅ Rate limiting and security middlewares
- ✅ Docker support
- ✅ Soft delete functionality
- ✅ Complete CRUD operations

The architecture is highly scalable and follows best practices for production microservices.