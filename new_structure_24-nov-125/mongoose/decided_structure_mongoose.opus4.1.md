I'll create a professional, enterprise-grade microservice architecture with Express.js, Mongoose, and Kafka event-driven pattern. Here's the complete code structure:

## Root Structure

File: `docker-compose.yml`

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

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
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_HOST://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_HOST
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  mongodb:
    image: mongo:7.0
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password123
      MONGO_INITDB_DATABASE: microservices
    volumes:
      - mongodb_data:/data/db

volumes:
  mongodb_data:
```

File: `.gitignore`

```gitignore
node_modules/
dist/
*.log
.env
.env.local
.env.*.local
.DS_Store
coverage/
.vscode/
.idea/
*.swp
*.swo
build/
```

File: `package.json`

```json
{
  "name": "microservices-root",
  "version": "1.0.0",
  "description": "Microservices architecture with Express, Mongoose, and Kafka",
  "scripts": {
    "dev:product": "cd services/products && npm run dev",
    "build:product": "cd services/products && npm run build",
    "start:product": "cd services/products && npm run start",
    "docker:up": "docker-compose up -d",
    "docker:down": "docker-compose down",
    "docker:logs": "docker-compose logs -f"
  },
  "keywords": ["microservices", "kafka", "express", "mongoose"],
  "author": "",
  "license": "ISC"
}
```

## Product Service

File: `services/products/package.json`

```json
{
  "name": "product-service",
  "version": "1.0.0",
  "description": "Product microservice",
  "main": "dist/server.js",
  "scripts": {
    "dev": "tsx watch src/server.ts",
    "build": "tsc",
    "start": "node dist/server.js",
    "lint": "eslint . --ext .ts",
    "test": "jest",
    "test:watch": "jest --watch"
  },
  "dependencies": {
    "express": "^4.18.2",
    "mongoose": "^8.0.3",
    "kafkajs": "^2.2.4",
    "zod": "^3.22.4",
    "dotenv": "^16.3.1",
    "cors": "^2.8.5",
    "helmet": "^7.1.0",
    "compression": "^1.7.4",
    "express-rate-limit": "^7.1.5",
    "winston": "^3.11.0",
    "uuid": "^9.0.1"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "@types/node": "^20.10.5",
    "@types/cors": "^2.8.17",
    "@types/compression": "^1.7.5",
    "@types/uuid": "^9.0.7",
    "typescript": "^5.3.3",
    "tsx": "^4.7.0",
    "@typescript-eslint/eslint-plugin": "^6.15.0",
    "@typescript-eslint/parser": "^6.15.0",
    "eslint": "^8.56.0",
    "jest": "^29.7.0",
    "@types/jest": "^29.5.11",
    "ts-jest": "^29.1.1"
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
    "typeRoots": ["./node_modules/@types", "./src/types"]
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist", "**/*.test.ts", "**/*.spec.ts"]
}
```

File: `services/products/.env.example`

```env
# Server Configuration
NODE_ENV=development
PORT=3001
SERVICE_NAME=product-service

# Database Configuration
MONGODB_URI=mongodb://admin:password123@localhost:27017/products?authSource=admin

# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=product-service
KAFKA_GROUP_ID=product-service-group

# Security
JWT_SECRET=your-secret-key-here
RATE_LIMIT_WINDOW_MS=900000
RATE_LIMIT_MAX_REQUESTS=100

# Logging
LOG_LEVEL=debug
```

File: `services/products/src/common/config.ts`

```typescript
import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.string().transform(Number).default('3001'),
  SERVICE_NAME: z.string().default('product-service'),
  MONGODB_URI: z.string().url().or(z.string().startsWith('mongodb://')),
  KAFKA_BROKERS: z.string().transform((val) => val.split(',')),
  KAFKA_CLIENT_ID: z.string(),
  KAFKA_GROUP_ID: z.string(),
  JWT_SECRET: z.string().optional(),
  RATE_LIMIT_WINDOW_MS: z.string().transform(Number).default('900000'),
  RATE_LIMIT_MAX_REQUESTS: z.string().transform(Number).default('100'),
  LOG_LEVEL: z.enum(['error', 'warn', 'info', 'debug']).default('info'),
});

const envVars = envSchema.parse(process.env);

export const config = {
  env: envVars.NODE_ENV,
  port: envVars.PORT,
  serviceName: envVars.SERVICE_NAME,
  mongodb: {
    uri: envVars.MONGODB_URI,
  },
  kafka: {
    brokers: envVars.KAFKA_BROKERS,
    clientId: envVars.KAFKA_CLIENT_ID,
    groupId: envVars.KAFKA_GROUP_ID,
  },
  security: {
    jwtSecret: envVars.JWT_SECRET,
    rateLimit: {
      windowMs: envVars.RATE_LIMIT_WINDOW_MS,
      maxRequests: envVars.RATE_LIMIT_MAX_REQUESTS,
    },
  },
  logging: {
    level: envVars.LOG_LEVEL,
  },
} as const;

export type Config = typeof config;
```

File: `services/products/src/common/database/connection.ts`

```typescript
import mongoose from 'mongoose';
import { config } from '../config';
import { logger } from '../logger';

export class DatabaseConnection {
  private static instance: DatabaseConnection;
  private connectionPromise: Promise<typeof mongoose> | null = null;

  private constructor() {}

  public static getInstance(): DatabaseConnection {
    if (!DatabaseConnection.instance) {
      DatabaseConnection.instance = new DatabaseConnection();
    }
    return DatabaseConnection.instance;
  }

  async connect(): Promise<typeof mongoose> {
    if (this.connectionPromise) {
      return this.connectionPromise;
    }

    this.connectionPromise = this.establishConnection();
    return this.connectionPromise;
  }

  private async establishConnection(): Promise<typeof mongoose> {
    try {
      mongoose.set('strictQuery', true);
      
      mongoose.connection.on('connected', () => {
        logger.info('MongoDB connected successfully');
      });

      mongoose.connection.on('error', (err) => {
        logger.error('MongoDB connection error:', err);
      });

      mongoose.connection.on('disconnected', () => {
        logger.warn('MongoDB disconnected');
      });

      const connection = await mongoose.connect(config.mongodb.uri, {
        maxPoolSize: 10,
        minPoolSize: 5,
        socketTimeoutMS: 45000,
        serverSelectionTimeoutMS: 5000,
        family: 4,
      });

      // Graceful shutdown
      process.on('SIGINT', async () => {
        await this.disconnect();
        process.exit(0);
      });

      return connection;
    } catch (error) {
      logger.error('Failed to connect to MongoDB:', error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    try {
      await mongoose.disconnect();
      logger.info('MongoDB disconnected gracefully');
    } catch (error) {
      logger.error('Error disconnecting from MongoDB:', error);
      throw error;
    }
  }

  getConnection(): mongoose.Connection {
    return mongoose.connection;
  }

  isConnected(): boolean {
    return mongoose.connection.readyState === 1;
  }
}

export const databaseConnection = DatabaseConnection.getInstance();
```

File: `services/products/src/common/kafka/client.ts`

```typescript
import { Kafka, Producer, Consumer, EachMessagePayload, Admin } from 'kafkajs';
import { config } from '../config';
import { logger } from '../logger';
import { v4 as uuidv4 } from 'uuid';

export interface KafkaEvent {
  id: string;
  timestamp: Date;
  source: string;
  type: string;
  data: any;
  metadata?: Record<string, any>;
}

export class KafkaClient {
  private static instance: KafkaClient;
  private kafka: Kafka;
  private producer: Producer | null = null;
  private consumers: Map<string, Consumer> = new Map();
  private admin: Admin | null = null;
  private isProducerConnected = false;

  private constructor() {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: config.kafka.brokers,
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
      connectionTimeout: 10000,
      requestTimeout: 30000,
    });
  }

  public static getInstance(): KafkaClient {
    if (!KafkaClient.instance) {
      KafkaClient.instance = new KafkaClient();
    }
    return KafkaClient.instance;
  }

  async connectProducer(): Promise<void> {
    if (this.isProducerConnected) {
      return;
    }

    try {
      this.producer = this.kafka.producer({
        allowAutoTopicCreation: true,
        transactionTimeout: 30000,
        compression: 1, // gzip
        retry: {
          initialRetryTime: 100,
          retries: 8,
        },
      });

      await this.producer.connect();
      this.isProducerConnected = true;
      logger.info('Kafka producer connected successfully');

      // Handle disconnection
      this.producer.on('producer.disconnect', () => {
        logger.warn('Kafka producer disconnected');
        this.isProducerConnected = false;
      });
    } catch (error) {
      logger.error('Failed to connect Kafka producer:', error);
      throw error;
    }
  }

  async publish(topic: string, event: Omit<KafkaEvent, 'id' | 'timestamp' | 'source'>): Promise<void> {
    if (!this.producer || !this.isProducerConnected) {
      await this.connectProducer();
    }

    const kafkaEvent: KafkaEvent = {
      id: uuidv4(),
      timestamp: new Date(),
      source: config.serviceName,
      ...event,
    };

    try {
      await this.producer!.send({
        topic,
        messages: [
          {
            key: kafkaEvent.id,
            value: JSON.stringify(kafkaEvent),
            headers: {
              'event-type': event.type,
              'source': config.serviceName,
              'timestamp': new Date().toISOString(),
            },
          },
        ],
      });

      logger.info(`Event published to topic ${topic}:`, {
        eventId: kafkaEvent.id,
        eventType: event.type,
      });
    } catch (error) {
      logger.error(`Failed to publish event to topic ${topic}:`, error);
      throw error;
    }
  }

  async subscribe(
    topic: string,
    handler: (message: EachMessagePayload) => Promise<void>,
    groupId?: string
  ): Promise<void> {
    const consumerGroupId = groupId || config.kafka.groupId;
    const consumerId = `${consumerGroupId}-${topic}`;

    if (this.consumers.has(consumerId)) {
      logger.warn(`Consumer already exists for ${consumerId}`);
      return;
    }

    const consumer = this.kafka.consumer({
      groupId: consumerGroupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
      maxBytesPerPartition: 1048576, // 1MB
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
    });

    try {
      await consumer.connect();
      await consumer.subscribe({ topic, fromBeginning: false });

      await consumer.run({
        autoCommit: true,
        autoCommitInterval: 5000,
        eachMessage: async (payload: EachMessagePayload) => {
          try {
            logger.debug(`Processing message from topic ${topic}:`, {
              partition: payload.partition,
              offset: payload.message.offset,
            });
            await handler(payload);
          } catch (error) {
            logger.error(`Error processing message from topic ${topic}:`, error);
            throw error;
          }
        },
      });

      this.consumers.set(consumerId, consumer);
      logger.info(`Consumer subscribed to topic ${topic} with group ${consumerGroupId}`);
    } catch (error) {
      logger.error(`Failed to subscribe to topic ${topic}:`, error);
      throw error;
    }
  }

  async createTopics(topics: string[]): Promise<void> {
    if (!this.admin) {
      this.admin = this.kafka.admin();
      await this.admin.connect();
    }

    try {
      const topicConfigs = topics.map(topic => ({
        topic,
        numPartitions: 3,
        replicationFactor: 1,
        configEntries: [
          { name: 'retention.ms', value: '604800000' }, // 7 days
          { name: 'compression.type', value: 'gzip' },
        ],
      }));

      await this.admin.createTopics({
        topics: topicConfigs,
        waitForLeaders: true,
      });

      logger.info('Topics created successfully:', topics);
    } catch (error: any) {
      if (error.message?.includes('already exists')) {
        logger.info('Topics already exist:', topics);
      } else {
        logger.error('Failed to create topics:', error);
        throw error;
      }
    }
  }

  async disconnect(): Promise<void> {
    try {
      if (this.producer) {
        await this.producer.disconnect();
        this.isProducerConnected = false;
      }

      for (const [id, consumer] of this.consumers.entries()) {
        await consumer.disconnect();
        this.consumers.delete(id);
      }

      if (this.admin) {
        await this.admin.disconnect();
      }

      logger.info('Kafka client disconnected successfully');
    } catch (error) {
      logger.error('Error disconnecting Kafka client:', error);
      throw error;
    }
  }
}

export const kafkaClient = KafkaClient.getInstance();
```

File: `services/products/src/common/logger/index.ts`

```typescript
import winston from 'winston';
import { config } from '../config';

const logFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.splat(),
  winston.format.json(),
  winston.format.printf(({ timestamp, level, message, ...metadata }) => {
    let msg = `${timestamp} [${level}] ${message}`;
    
    if (Object.keys(metadata).length > 0) {
      msg += ` ${JSON.stringify(metadata)}`;
    }
    
    return msg;
  })
);

const transports: winston.transport[] = [
  new winston.transports.Console({
    format: winston.format.combine(
      winston.format.colorize(),
      logFormat
    ),
  }),
];

if (config.env === 'production') {
  transports.push(
    new winston.transports.File({
      filename: 'logs/error.log',
      level: 'error',
      maxsize: 5242880, // 5MB
      maxFiles: 5,
    }),
    new winston.transports.File({
      filename: 'logs/combined.log',
      maxsize: 5242880, // 5MB
      maxFiles: 5,
    })
  );
}

export const logger = winston.createLogger({
  level: config.logging.level,
  format: logFormat,
  transports,
  exitOnError: false,
});

// Handle uncaught exceptions and unhandled rejections
logger.exceptions.handle(
  new winston.transports.File({ filename: 'logs/exceptions.log' })
);

logger.rejections.handle(
  new winston.transports.File({ filename: 'logs/rejections.log' })
);
```

File: `services/products/src/common/middlewares/error-handler.ts`

```typescript
import { Request, Response, NextFunction } from 'express';
import { ZodError } from 'zod';
import { logger } from '../logger';

export interface ApiError extends Error {
  statusCode?: number;
  isOperational?: boolean;
  details?: any;
}

export class AppError extends Error implements ApiError {
  statusCode: number;
  isOperational: boolean;
  details?: any;

  constructor(message: string, statusCode: number = 500, isOperational: boolean = true, details?: any) {
    super(message);
    this.statusCode = statusCode;
    this.isOperational = isOperational;
    this.details = details;
    Error.captureStackTrace(this, this.constructor);
  }
}

export const errorHandler = (err: ApiError, req: Request, res: Response, next: NextFunction) => {
  let statusCode = err.statusCode || 500;
  let message = err.message || 'Internal Server Error';
  let details = err.details;

  // Handle Zod validation errors
  if (err instanceof ZodError) {
    statusCode = 400;
    message = 'Validation Error';
    details = err.errors.map(error => ({
      field: error.path.join('.'),
      message: error.message,
    }));
  }

  // Handle Mongoose validation errors
  if (err.name === 'ValidationError') {
    statusCode = 400;
    message = 'Validation Error';
  }

  // Handle Mongoose duplicate key error
  if (err.name === 'MongoServerError' && (err as any).code === 11000) {
    statusCode = 409;
    message = 'Duplicate field value';
  }

  // Handle JWT errors
  if (err.name === 'JsonWebTokenError') {
    statusCode = 401;
    message = 'Invalid token';
  }

  if (err.name === 'TokenExpiredError') {
    statusCode = 401;
    message = 'Token expired';
  }

  // Log error
  logger.error('Error occurred:', {
    error: message,
    statusCode,
    path: req.path,
    method: req.method,
    ip: req.ip,
    stack: err.stack,
  });

  // Send error response
  res.status(statusCode).json({
    success: false,
    error: {
      message,
      statusCode,
      details,
      ...(process.env.NODE_ENV === 'development' && { stack: err.stack }),
    },
    timestamp: new Date().toISOString(),
  });
};

export const notFound = (req: Request, res: Response, next: NextFunction) => {
  const error = new AppError(`Not found - ${req.originalUrl}`, 404);
  next(error);
};

export const asyncHandler = (fn: Function) => {
  return (req: Request, res: Response, next: NextFunction) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
};
```

File: `services/products/src/common/middlewares/request-logger.ts`

```typescript
import { Request, Response, NextFunction } from 'express';
import { logger } from '../logger';
import { v4 as uuidv4 } from 'uuid';

export interface RequestWithId extends Request {
  id?: string;
  startTime?: number;
}

export const requestLogger = (req: RequestWithId, res: Response, next: NextFunction) => {
  req.id = uuidv4();
  req.startTime = Date.now();

  logger.info('Incoming request', {
    requestId: req.id,
    method: req.method,
    url: req.url,
    ip: req.ip,
    userAgent: req.get('user-agent'),
  });

  res.on('finish', () => {
    const duration = Date.now() - req.startTime!;
    
    logger.info('Request completed', {
      requestId: req.id,
      method: req.method,
      url: req.url,
      statusCode: res.statusCode,
      duration: `${duration}ms`,
    });
  });

  next();
};
```

File: `services/products/src/common/middlewares/rate-limiter.ts`

```typescript
import rateLimit from 'express-rate-limit';
import { config } from '../config';

export const createRateLimiter = (maxRequests?: number, windowMs?: number) => {
  return rateLimit({
    windowMs: windowMs || config.security.rateLimit.windowMs,
    max: maxRequests || config.security.rateLimit.maxRequests,
    message: 'Too many requests from this IP, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
    handler: (req, res) => {
      res.status(429).json({
        success: false,
        error: {
          message: 'Too many requests',
          statusCode: 429,
          retryAfter: res.get('Retry-After'),
        },
      });
    },
  });
};

export const apiLimiter = createRateLimiter();
```

File: `services/products/src/common/middlewares/index.ts`

```typescript
export * from './error-handler';
export * from './request-logger';
export * from './rate-limiter';
```

File: `services/products/src/modules/product/product.model.ts`

```typescript
import mongoose, { Schema, Document } from 'mongoose';

export interface IProduct {
  name: string;
  slug: string;
  description?: string;
  sku: string;
  price: number;
  compareAtPrice?: number;
  costPrice?: number;
  quantity: number;
  trackQuantity: boolean;
  weight?: number;
  dimensions?: {
    length?: number;
    width?: number;
    height?: number;
  };
  images: string[];
  featured: boolean;
  status: 'active' | 'inactive' | 'draft';
  categoryId?: string;
  brandId?: string;
  tags: string[];
  attributes: Map<string, any>;
  seo?: {
    title?: string;
    description?: string;
    keywords?: string[];
  };
  createdBy?: string;
  updatedBy?: string;
}

export interface IProductDocument extends IProduct, Document {
  createdAt: Date;
  updatedAt: Date;
}

const productSchema = new Schema<IProductDocument>(
  {
    name: {
      type: String,
      required: [true, 'Product name is required'],
      trim: true,
      maxlength: [200, 'Product name cannot be more than 200 characters'],
      index: true,
    },
    slug: {
      type: String,
      required: [true, 'Product slug is required'],
      unique: true,
      lowercase: true,
      trim: true,
      index: true,
    },
    description: {
      type: String,
      maxlength: [5000, 'Description cannot be more than 5000 characters'],
    },
    sku: {
      type: String,
      required: [true, 'SKU is required'],
      unique: true,
      uppercase: true,
      trim: true,
      index: true,
    },
    price: {
      type: Number,
      required: [true, 'Price is required'],
      min: [0, 'Price cannot be negative'],
      get: (v: number) => parseFloat(v.toFixed(2)),
      set: (v: number) => parseFloat(v.toFixed(2)),
    },
    compareAtPrice: {
      type: Number,
      min: [0, 'Compare at price cannot be negative'],
      get: (v: number) => v ? parseFloat(v.toFixed(2)) : v,
      set: (v: number) => v ? parseFloat(v.toFixed(2)) : v,
    },
    costPrice: {
      type: Number,
      min: [0, 'Cost price cannot be negative'],
      get: (v: number) => v ? parseFloat(v.toFixed(2)) : v,
      set: (v: number) => v ? parseFloat(v.toFixed(2)) : v,
    },
    quantity: {
      type: Number,
      required: [true, 'Quantity is required'],
      min: [0, 'Quantity cannot be negative'],
      default: 0,
    },
    trackQuantity: {
      type: Boolean,
      default: true,
    },
    weight: {
      type: Number,
      min: [0, 'Weight cannot be negative'],
    },
    dimensions: {
      length: { type: Number, min: 0 },
      width: { type: Number, min: 0 },
      height: { type: Number, min: 0 },
    },
    images: {
      type: [String],
      default: [],
      validate: {
        validator: function(v: string[]) {
          return v.length <= 10;
        },
        message: 'Product cannot have more than 10 images',
      },
    },
    featured: {
      type: Boolean,
      default: false,
      index: true,
    },
    status: {
      type: String,
      enum: ['active', 'inactive', 'draft'],
      default: 'draft',
      index: true,
    },
    categoryId: {
      type: String,
      index: true,
    },
    brandId: {
      type: String,
      index: true,
    },
    tags: {
      type: [String],
      default: [],
      index: true,
    },
    attributes: {
      type: Map,
      of: Schema.Types.Mixed,
      default: new Map(),
    },
    seo: {
      title: String,
      description: String,
      keywords: [String],
    },
    createdBy: {
      type: String,
    },
    updatedBy: {
      type: String,
    },
  },
  {
    timestamps: true,
    toJSON: {
      getters: true,
      virtuals: true,
      transform: function(doc, ret) {
        delete ret.__v;
        return ret;
      },
    },
    toObject: {
      getters: true,
      virtuals: true,
    },
  }
);

// Indexes for better query performance
productSchema.index({ name: 'text', description: 'text' });
productSchema.index({ price: 1, status: 1 });
productSchema.index({ createdAt: -1 });
productSchema.index({ status: 1, featured: 1 });

// Virtual for display price (with currency formatting)
productSchema.virtual('displayPrice').get(function(this: IProductDocument) {
  return `$${this.price.toFixed(2)}`;
});

// Virtual for discount percentage
productSchema.virtual('discountPercentage').get(function(this: IProductDocument) {
  if (this.compareAtPrice && this.compareAtPrice > this.price) {
    const discount = ((this.compareAtPrice - this.price) / this.compareAtPrice) * 100;
    return Math.round(discount);
  }
  return 0;
});

export const ProductModel = mongoose.model<IProductDocument>('Product', productSchema);
```

File: `services/products/src/modules/product/product.schema.ts`

```typescript
import { z } from 'zod';

export const createProductSchema = z.object({
  name: z.string().min(1).max(200),
  slug: z.string().min(1).regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/, 'Invalid slug format'),
  description: z.string().max(5000).optional(),
  sku: z.string().min(1),
  price: z.number().min(0),
  compareAtPrice: z.number().min(0).optional(),
  costPrice: z.number().min(0).optional(),
  quantity: z.number().int().min(0).default(0),
  trackQuantity: z.boolean().default(true),
  weight: z.number().min(0).optional(),
  dimensions: z.object({
    length: z.number().min(0).optional(),
    width: z.number().min(0).optional(),
    height: z.number().min(0).optional(),
  }).optional(),
  images: z.array(z.string().url()).max(10).default([]),
  featured: z.boolean().default(false),
  status: z.enum(['active', 'inactive', 'draft']).default('draft'),
  categoryId: z.string().optional(),
  brandId: z.string().optional(),
  tags: z.array(z.string()).default([]),
  attributes: z.record(z.any()).default({}),
  seo: z.object({
    title: z.string().optional(),
    description: z.string().optional(),
    keywords: z.array(z.string()).optional(),
  }).optional(),
  createdBy: z.string().optional(),
});

export const updateProductSchema = createProductSchema.partial().extend({
  updatedBy: z.string().optional(),
});

export const productQuerySchema = z.object({
  page: z.string().transform(Number).default('1'),
  limit: z.string().transform(Number).default('10'),
  sort: z.string().optional(),
  status: z.enum(['active', 'inactive', 'draft']).optional(),
  featured: z.string().transform(val => val === 'true').optional(),
  categoryId: z.string().optional(),
  brandId: z.string().optional(),
  tag: z.string().optional(),
  search: z.string().optional(),
  minPrice: z.string().transform(Number).optional(),
  maxPrice: z.string().transform(Number).optional(),
});

export type CreateProductDTO = z.infer<typeof createProductSchema>;
export type UpdateProductDTO = z.infer<typeof updateProductSchema>;
export type ProductQueryDTO = z.infer<typeof productQuerySchema>;
```

File: `services/products/src/modules/product/events/product-created.producer.ts`

```typescript
import { kafkaClient } from '../../../common/kafka/client';
import { IProductDocument } from '../product.model';

export const PRODUCT_CREATED_TOPIC = 'product.created';

export interface ProductCreatedEvent {
  productId: string;
  name: string;
  sku: string;
  price: number;
  quantity: number;
  status: string;
  categoryId?: string;
  brandId?: string;
  createdAt: Date;
}

export class ProductCreatedProducer {
  static async publish(product: IProductDocument): Promise<void> {
    const event: ProductCreatedEvent = {
      productId: product._id.toString(),
      name: product.name,
      sku: product.sku,
      price: product.price,
      quantity: product.quantity,
      status: product.status,
      categoryId: product.categoryId,
      brandId: product.brandId,
      createdAt: product.createdAt,
    };

    await kafkaClient.publish(PRODUCT_CREATED_TOPIC, {
      type: 'ProductCreated',
      data: event,
      metadata: {
        source: 'product-service',
        version: '1.0.0',
      },
    });
  }
}
```

File: `services/products/src/modules/product/events/product-updated.producer.ts`

```typescript
import { kafkaClient } from '../../../common/kafka/client';
import { IProductDocument } from '../product.model';

export const PRODUCT_UPDATED_TOPIC = 'product.updated';

export interface ProductUpdatedEvent {
  productId: string;
  changes: {
    field: string;
    oldValue: any;
    newValue: any;
  }[];
  updatedFields: string[];
  updatedAt: Date;
}

export class ProductUpdatedProducer {
  static async publish(
    product: IProductDocument,
    previousData: Partial<IProductDocument>
  ): Promise<void> {
    const changes: ProductUpdatedEvent['changes'] = [];
    const updatedFields: string[] = [];

    // Compare and track changes
    Object.keys(previousData).forEach(key => {
      if (product[key as keyof IProductDocument] !== previousData[key as keyof IProductDocument]) {
        updatedFields.push(key);
        changes.push({
          field: key,
          oldValue: previousData[key as keyof IProductDocument],
          newValue: product[key as keyof IProductDocument],
        });
      }
    });

    if (changes.length === 0) return;

    const event: ProductUpdatedEvent = {
      productId: product._id.toString(),
      changes,
      updatedFields,
      updatedAt: product.updatedAt,
    };

    await kafkaClient.publish(PRODUCT_UPDATED_TOPIC, {
      type: 'ProductUpdated',
      data: event,
      metadata: {
        source: 'product-service',
        version: '1.0.0',
      },
    });
  }
}
```

File: `services/products/src/modules/product/events/product-deleted.producer.ts`

```typescript
import { kafkaClient } from '../../../common/kafka/client';

export const PRODUCT_DELETED_TOPIC = 'product.deleted';

export interface ProductDeletedEvent {
  productId: string;
  sku: string;
  deletedAt: Date;
  deletedBy?: string;
}

export class ProductDeletedProducer {
  static async publish(productId: string, sku: string, deletedBy?: string): Promise<void> {
    const event: ProductDeletedEvent = {
      productId,
      sku,
      deletedAt: new Date(),
      deletedBy,
    };

    await kafkaClient.publish(PRODUCT_DELETED_TOPIC, {
      type: 'ProductDeleted',
      data: event,
      metadata: {
        source: 'product-service',
        version: '1.0.0',
      },
    });
  }
}
```

File: `services/products/src/modules/product/events/inventory-updated.consumer.ts`

```typescript
import { EachMessagePayload } from 'kafkajs';
import { kafkaClient, KafkaEvent } from '../../../common/kafka/client';
import { ProductModel } from '../product.model';
import { logger } from '../../../common/logger';

export const INVENTORY_UPDATED_TOPIC = 'inventory.updated';

export interface InventoryUpdatedEvent {
  productId: string;
  sku: string;
  quantity: number;
  previousQuantity: number;
  reason: string;
  updatedAt: Date;
}

export class InventoryUpdatedConsumer {
  static async subscribe(): Promise<void> {
    await kafkaClient.subscribe(
      INVENTORY_UPDATED_TOPIC,
      InventoryUpdatedConsumer.handleMessage
    );
  }

  static async handleMessage(payload: EachMessagePayload): Promise<void> {
    try {
      const { message } = payload;
      const event: KafkaEvent = JSON.parse(message.value?.toString() || '{}');
      const inventoryData = event.data as InventoryUpdatedEvent;

      logger.info('Processing inventory update event:', {
        productId: inventoryData.productId,
        newQuantity: inventoryData.quantity,
      });

      // Update product quantity based on inventory service event
      const product = await ProductModel.findByIdAndUpdate(
        inventoryData.productId,
        {
          quantity: inventoryData.quantity,
          updatedAt: new Date(),
        },
        { new: true }
      );

      if (!product) {
        logger.warn('Product not found for inventory update:', {
          productId: inventoryData.productId,
        });
        return;
      }

      logger.info('Product inventory updated successfully:', {
        productId: product._id,
        sku: product.sku,
        newQuantity: product.quantity,
      });
    } catch (error) {
      logger.error('Error processing inventory update event:', error);
      throw error;
    }
  }
}
```

File: `services/products/src/modules/product/product.controller.ts`

```typescript
import { Request, Response } from 'express';
import { ProductModel, IProductDocument } from './product.model';
import {
  createProductSchema,
  updateProductSchema,
  productQuerySchema,
  CreateProductDTO,
  UpdateProductDTO,
  ProductQueryDTO,
} from './product.schema';
import { ProductCreatedProducer } from './events/product-created.producer';
import { ProductUpdatedProducer } from './events/product-updated.producer';
import { ProductDeletedProducer } from './events/product-deleted.producer';
import { AppError } from '../../common/middlewares';
import { logger } from '../../common/logger';

export class ProductController {
  // Create a new product
  static async createProduct(req: Request, res: Response): Promise<void> {
    try {
      // Validate request body
      const validatedData: CreateProductDTO = createProductSchema.parse(req.body);

      // Check for duplicate SKU
      const existingProduct = await ProductModel.findOne({ sku: validatedData.sku });
      if (existingProduct) {
        throw new AppError('Product with this SKU already exists', 409);
      }

      // Check for duplicate slug
      const existingSlug = await ProductModel.findOne({ slug: validatedData.slug });
      if (existingSlug) {
        throw new AppError('Product with this slug already exists', 409);
      }

      // Create product
      const product = await ProductModel.create({
        ...validatedData,
        attributes: new Map(Object.entries(validatedData.attributes || {})),
      });

      // Publish event to Kafka
      await ProductCreatedProducer.publish(product);

      logger.info('Product created successfully:', { productId: product._id, sku: product.sku });

      res.status(201).json({
        success: true,
        data: product,
        message: 'Product created successfully',
      });
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      logger.error('Error creating product:', error);
      throw new AppError('Failed to create product', 500, false, error);
    }
  }

  // Get all products with pagination and filters
  static async getProducts(req: Request, res: Response): Promise<void> {
    try {
      // Validate query parameters
      const query: ProductQueryDTO = productQuerySchema.parse(req.query);

      // Build filter
      const filter: any = {};
      
      if (query.status) filter.status = query.status;
      if (query.featured !== undefined) filter.featured = query.featured;
      if (query.categoryId) filter.categoryId = query.categoryId;
      if (query.brandId) filter.brandId = query.brandId;
      if (query.tag) filter.tags = { $in: [query.tag] };
      
      if (query.minPrice || query.maxPrice) {
        filter.price = {};
        if (query.minPrice) filter.price.$gte = query.minPrice;
        if (query.maxPrice) filter.price.$lte = query.maxPrice;
      }

      if (query.search) {
        filter.$text = { $search: query.search };
      }

      // Build sort
      let sortOptions: any = { createdAt: -1 };
      if (query.sort) {
        const sortField = query.sort.startsWith('-') ? query.sort.slice(1) : query.sort;
        const sortOrder = query.sort.startsWith('-') ? -1 : 1;
        sortOptions = { [sortField]: sortOrder };
      }

      // Execute query with pagination
      const skip = (query.page - 1) * query.limit;
      const [products, total] = await Promise.all([
        ProductModel.find(filter)
          .sort(sortOptions)
          .skip(skip)
          .limit(query.limit)
          .lean(),
        ProductModel.countDocuments(filter),
      ]);

      res.status(200).json({
        success: true,
        data: products,
        pagination: {
          page: query.page,
          limit: query.limit,
          total,
          totalPages: Math.ceil(total / query.limit),
        },
      });
    } catch (error) {
      logger.error('Error fetching products:', error);
      throw new AppError('Failed to fetch products', 500, false, error);
    }
  }

  // Get single product by ID
  static async getProductById(req: Request, res: Response): Promise<void> {
    try {
      const { id } = req.params;

      const product = await ProductModel.findById(id);
      if (!product) {
        throw new AppError('Product not found', 404);
      }

      res.status(200).json({
        success: true,
        data: product,
      });
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      logger.error('Error fetching product:', error);
      throw new AppError('Failed to fetch product', 500, false, error);
    }
  }

  // Get product by slug
  static async getProductBySlug(req: Request, res: Response): Promise<void> {
    try {
      const { slug } = req.params;

      const product = await ProductModel.findOne({ slug });
      if (!product) {
        throw new AppError('Product not found', 404);
      }

      res.status(200).json({
        success: true,
        data: product,
      });
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      logger.error('Error fetching product by slug:', error);
      throw new AppError('Failed to fetch product', 500, false, error);
    }
  }

  // Update product
  static async updateProduct(req: Request, res: Response): Promise<void> {
    try {
      const { id } = req.params;
      
      // Validate request body
      const validatedData: UpdateProductDTO = updateProductSchema.parse(req.body);

      // Get current product data for comparison
      const currentProduct = await ProductModel.findById(id);
      if (!currentProduct) {
        throw new AppError('Product not found', 404);
      }

      // Store previous data for event
      const previousData = currentProduct.toObject();

      // Check for SKU uniqueness if updating
      if (validatedData.sku && validatedData.sku !== currentProduct.sku) {
        const existingProduct = await ProductModel.findOne({ sku: validatedData.sku });
        if (existingProduct) {
          throw new AppError('Product with this SKU already exists', 409);
        }
      }

      // Check for slug uniqueness if updating
      if (validatedData.slug && validatedData.slug !== currentProduct.slug) {
        const existingSlug = await ProductModel.findOne({ slug: validatedData.slug });
        if (existingSlug) {
          throw new AppError('Product with this slug already exists', 409);
        }
      }

      // Update product
      const updatedProduct = await ProductModel.findByIdAndUpdate(
        id,
        {
          ...validatedData,
          ...(validatedData.attributes && {
            attributes: new Map(Object.entries(validatedData.attributes)),
          }),
        },
        { new: true, runValidators: true }
      );

      if (!updatedProduct) {
        throw new AppError('Failed to update product', 500);
      }

      // Publish update event to Kafka
      await ProductUpdatedProducer.publish(updatedProduct, previousData);

      logger.info('Product updated successfully:', { productId: id });

      res.status(200).json({
        success: true,
        data: updatedProduct,
        message: 'Product updated successfully',
      });
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      logger.error('Error updating product:', error);
      throw new AppError('Failed to update product', 500, false, error);
    }
  }

  // Delete product
  static async deleteProduct(req: Request, res: Response): Promise<void> {
    try {
      const { id } = req.params;

      const product = await ProductModel.findById(id);
      if (!product) {
        throw new AppError('Product not found', 404);
      }

      await product.deleteOne();

      // Publish delete event to Kafka
      await ProductDeletedProducer.publish(
        product._id.toString(),
        product.sku,
        req.body.deletedBy
      );

      logger.info('Product deleted successfully:', { productId: id });

      res.status(200).json({
        success: true,
        message: 'Product deleted successfully',
      });
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      logger.error('Error deleting product:', error);
      throw new AppError('Failed to delete product', 500, false, error);
    }
  }

  // Bulk update product status
  static async bulkUpdateStatus(req: Request, res: Response): Promise<void> {
    try {
      const { productIds, status } = req.body;

      if (!Array.isArray(productIds) || productIds.length === 0) {
        throw new AppError('Product IDs must be a non-empty array', 400);
      }

      if (!['active', 'inactive', 'draft'].includes(status)) {
        throw new AppError('Invalid status value', 400);
      }

      const result = await ProductModel.updateMany(
        { _id: { $in: productIds } },
        { status, updatedAt: new Date() }
      );

      logger.info('Bulk status update completed:', {
        matchedCount: result.matchedCount,
        modifiedCount: result.modifiedCount,
      });

      res.status(200).json({
        success: true,
        data: {
          matchedCount: result.matchedCount,
          modifiedCount: result.modifiedCount,
        },
        message: 'Products status updated successfully',
      });
    } catch (error) {
      if (error instanceof AppError) {
        throw error;
      }
      logger.error('Error in bulk status update:', error);
      throw new AppError('Failed to update products status', 500, false, error);
    }
  }
}
```

File: `services/products/src/modules/product/product.routes.ts`

```typescript
import { Router } from 'express';
import { ProductController } from './product.controller';
import { asyncHandler } from '../../common/middlewares';
import { createRateLimiter } from '../../common/middlewares/rate-limiter';

const router = Router();

// Rate limiters for different operations
const readLimiter = createRateLimiter(100, 60000); // 100 requests per minute
const writeLimiter = createRateLimiter(20, 60000); // 20 requests per minute

// GET routes
router.get(
  '/',
  readLimiter,
  asyncHandler(ProductController.getProducts)
);

router.get(
  '/:id',
  readLimiter,
  asyncHandler(ProductController.getProductById)
);

router.get(
  '/slug/:slug',
  readLimiter,
  asyncHandler(ProductController.getProductBySlug)
);

// POST routes
router.post(
  '/',
  writeLimiter,
  asyncHandler(ProductController.createProduct)
);

router.post(
  '/bulk-status',
  writeLimiter,
  asyncHandler(ProductController.bulkUpdateStatus)
);

// PUT routes
router.put(
  '/:id',
  writeLimiter,
  asyncHandler(ProductController.updateProduct)
);

// DELETE routes
router.delete(
  '/:id',
  writeLimiter,
  asyncHandler(ProductController.deleteProduct)
);

export default router;
```

File: `services/products/src/app.ts`

```typescript
import express, { Application } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import { requestLogger, errorHandler, notFound } from './common/middlewares';
import { apiLimiter } from './common/middlewares/rate-limiter';

// Module routes
import productRoutes from './modules/product/product.routes';

export const createApp = (): Application => {
  const app = express();

  // Trust proxy
  app.set('trust proxy', 1);

  // Security middlewares
  app.use(helmet());
  app.use(cors({
    origin: process.env.CORS_ORIGIN?.split(',') || '*',
    credentials: true,
  }));

  // Body parsing middlewares
  app.use(express.json({ limit: '10mb' }));
  app.use(express.urlencoded({ extended: true, limit: '10mb' }));

  // Compression
  app.use(compression());

  // Request logging
  app.use(requestLogger);

  // Global rate limiting
  app.use('/api/', apiLimiter);

  // Health check
  app.get('/health', (req, res) => {
    res.status(200).json({
      success: true,
      service: 'product-service',
      status: 'healthy',
      timestamp: new Date().toISOString(),
    });
  });

  // API routes
  app.use('/api/v1/products', productRoutes);

  // 404 handler
  app.use(notFound);

  // Error handler
  app.use(errorHandler);

  return app;
};
```

File: `services/products/src/server.ts`

```typescript
import { createApp } from './app';
import { config } from './common/config';
import { logger } from './common/logger';
import { databaseConnection } from './common/database/connection';
import { kafkaClient } from './common/kafka/client';
import { InventoryUpdatedConsumer } from './modules/product/events/inventory-updated.consumer';

const startServer = async (): Promise<void> => {
  try {
    // Connect to MongoDB
    await databaseConnection.connect();
    logger.info('MongoDB connected successfully');

    // Connect Kafka producer
    await kafkaClient.connectProducer();
    logger.info('Kafka producer connected successfully');

    // Create Kafka topics
    await kafkaClient.createTopics([
      'product.created',
      'product.updated',
      'product.deleted',
      'inventory.updated',
    ]);

    // Subscribe to Kafka consumers
    await InventoryUpdatedConsumer.subscribe();
    logger.info('Kafka consumers subscribed successfully');

    // Create Express app
    const app = createApp();

    // Start server
    const server = app.listen(config.port, () => {
      logger.info(`🚀 ${config.serviceName} is running on port ${config.port}`);
      logger.info(`Environment: ${config.env}`);
    });

    // Graceful shutdown
    const gracefulShutdown = async (signal: string): Promise<void> => {
      logger.info(`${signal} received, starting graceful shutdown`);

      server.close(() => {
        logger.info('HTTP server closed');
      });

      await kafkaClient.disconnect();
      await databaseConnection.disconnect();

      process.exit(0);
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));

    // Handle uncaught errors
    process.on('unhandledRejection', (reason: any, promise: Promise<any>) => {
      logger.error('Unhandled Rejection at:', { promise, reason });
      gracefulShutdown('UNHANDLED_REJECTION');
    });

    process.on('uncaughtException', (error: Error) => {
      logger.error('Uncaught Exception:', error);
      gracefulShutdown('UNCAUGHT_EXCEPTION');
    });

  } catch (error) {
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
};

startServer();
```

File: `services/products/.eslintrc.json`

```json
{
  "parser": "@typescript-eslint/parser",
  "extends": [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended"
  ],
  "parserOptions": {
    "ecmaVersion": 2022,
    "sourceType": "module"
  },
  "rules": {
    "@typescript-eslint/explicit-function-return-type": "off",
    "@typescript-eslint/no-explicit-any": "warn",
    "@typescript-eslint/no-unused-vars": ["error", { "argsIgnorePattern": "^_" }],
    "no-console": "warn",
    "prefer-const": "error"
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
    '!src/**/*.test.ts',
    '!src/**/*.spec.ts',
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
};
```

File: `README.md`

```markdown
# Microservices Architecture with Express, Mongoose & Kafka

## Overview
Enterprise-grade microservices architecture built with Express.js, MongoDB/Mongoose, and Apache Kafka for event-driven communication.

## Tech Stack
- **Runtime**: Node.js with TypeScript
- **Framework**: Express.js
- **Database**: MongoDB with Mongoose ODM
- **Message Broker**: Apache Kafka
- **Validation**: Zod
- **Logging**: Winston
- **Development**: TSX for hot reloading

## Project Structure
```
.
├── services/
│   ├── products/       # Product microservice
│   ├── inventory/      # Inventory microservice (to be implemented)
│   ├── orders/         # Orders microservice (to be implemented)
│   └── users/          # Users microservice (to be implemented)
├── docker-compose.yml  # Local development infrastructure
└── package.json        # Root package.json for scripts
```

## Getting Started

### Prerequisites
- Node.js 18+ 
- Docker & Docker Compose
- MongoDB (via Docker or local installation)
- Apache Kafka (via Docker)

### Installation

1. Clone the repository
```bash
git clone <repository-url>
cd microservices-architecture
```

2. Start infrastructure services
```bash
npm run docker:up
```

3. Install dependencies for Product Service
```bash
cd services/products
npm install
```

4. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Run Product Service in development mode
```bash
npm run dev
```

## Available Scripts

### Root Level
- `npm run docker:up` - Start Docker containers (MongoDB, Kafka, Zookeeper)
- `npm run docker:down` - Stop Docker containers
- `npm run dev:product` - Run Product service in development mode
- `npm run build:product` - Build Product service for production

### Service Level (Product Service)
- `npm run dev` - Start development server with hot reload
- `npm run build` - Build for production
- `npm run start` - Start production server
- `npm run test` - Run tests
- `npm run lint` - Run ESLint

## API Endpoints

### Product Service (Port 3001)

- `GET /health` - Health check
- `GET /api/v1/products` - Get all products (with pagination & filters)
- `GET /api/v1/products/:id` - Get product by ID
- `GET /api/v1/products/slug/:slug` - Get product by slug
- `POST /api/v1/products` - Create new product
- `PUT /api/v1/products/:id` - Update product
- `DELETE /api/v1/products/:id` - Delete product
- `POST /api/v1/products/bulk-status` - Bulk update product status

## Event-Driven Architecture

### Kafka Topics

#### Publishers
- `product.created` - Published when a new product is created
- `product.updated` - Published when a product is updated
- `product.deleted` - Published when a product is deleted

#### Consumers
- `inventory.updated` - Consumed to update product quantity

## Development Guidelines

### Code Structure
- **No Repository/Service Layers**: Business logic is embedded directly in controllers
- **Validation**: Use Zod schemas for request/response validation
- **Error Handling**: Centralized error handling with custom AppError class
- **Logging**: Structured logging with Winston
- **Events**: Kafka producers/consumers for inter-service communication

### Best Practices
1. Always validate input using Zod schemas
2. Use async/await with proper error handling
3. Implement rate limiting for API endpoints
4. Log all important operations and errors
5. Publish events for all state changes
6. Handle Kafka consumer errors gracefully
7. Implement graceful shutdown

## Production Deployment

### Environment Variables
Ensure all required environment variables are set in production:
- `NODE_ENV=production`
- `MONGODB_URI` - Production MongoDB connection string
- `KAFKA_BROKERS` - Comma-separated list of Kafka brokers
- `JWT_SECRET` - Strong secret for JWT tokens

### Build & Deploy
```bash
# Build the service
npm run build

# Start production server
NODE_ENV=production npm start
```

### Monitoring
- Health checks available at `/health`
- Structured logging with Winston
- Metrics collection ready (implement with Prometheus)

## Contributing
1. Follow the existing code structure
2. Write tests for new features
3. Use conventional commit messages
4. Ensure all tests pass before submitting PR

## License
ISC
```

This is a complete, production-ready microservice architecture with Express.js, Mongoose, and Kafka. The code follows professional standards with:

1. **Enterprise-grade structure** with clear separation of concerns
2. **TypeScript** for type safety
3. **Comprehensive error handling** and logging
4. **Event-driven architecture** with Kafka
5. **Input validation** with Zod
6. **Rate limiting** and security best practices
7. **Graceful shutdown** handling
8. **Docker setup** for local development
9. **Scalable patterns** that can be replicated for other services

You can now:
- Run `docker-compose up -d` to start infrastructure
- Navigate to `services/products` and run `npm install`
- Copy `.env.example` to `.env` and configure
- Run `npm run dev` to start the service

The architecture is ready to be extended with inventory, orders, and other services following the same pattern.