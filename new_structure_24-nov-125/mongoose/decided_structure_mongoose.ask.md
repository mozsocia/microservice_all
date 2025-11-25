in microservice architecture using express js and mongoose kafka  event-driven architecture , i have givne setup for service file and folder structure to high professinal scallable standard way, 

i want the top enginners code structure  Development & Production

i have given below one service file and folder 

**Example: An `product Service` with this structure:**

```text
services/products/src
├── common/                   # Shared logic GLOBAL to this microservice
│   ├── middlewares/          # Error handlers, Logger middleware
│   ├── database/             # Mongoose connection logic
│   ├── kafka/                # Kafka connection wrapper (The "Client")
│   └── config.ts
│
├── modules/                  # THE CORE DOMAIN LOGIC
│   │
│   ├── product/              # <--- The "Product" Feature
│   │   ├── events/           # Kafka interactions specific to Products
│   │   │   ├── product-created.producer.ts
│   │   │   ├── product-updated.producer.ts
│   │   │   ├── product-deleted.producer.ts
│   │   │   └── inventory-updated.consumer.ts  <-- Handler for inventory changes
│   │   ├── product.schema.ts  # <--- All Zod schemas for validation
│   │   ├── product.controller.ts  # <--- ALL LOGIC HERE (DB + Kafka + Validation)
│   │   ├── product.model.ts       <-- Mongoose Schema
│   │   └── product.routes.ts
│   ├── category/             
│   ├── brand/  
│   └── tag/  
│
├── app.ts                    # Register module routes here
└── server.ts                 # Entry point

services/inventory/
├── common/
├── modules/
├── app.ts             
└── server.ts 
```

### example controller codes, follow this pattern for controller
```js
       // Controller logic embedded here
       const createProduct = async (req, res) => {
         try {
           const validated = productSchema.parse(req.body); // Zod validation
           const product = await ProductModel.create(validated);
           // Kafka produce event...
           res.status(201).json(product);
         } catch (err) {
           res.status(400).json({ error: err.message });
         }
       };
```





do not use repository service layer , i do not want thoose layer
do not give category brand tag module  or inventory services any codes , i will make them later

every service will have there package.json tsconfig.json .env seperate in the service folder

now give me full codes of this project , a root folder to hold all service and products service folder with only product module , i will make other service and modules later,

make it high level professional and enterprise grade


Generate the new code now, for code part start with the "File: [path]" line, followed by the code block wraped by triple backticks with the language identifier. Add New line before and after file path and code blocks.

Let's think step by step