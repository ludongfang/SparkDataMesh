# Data Contract DSL Guide

A comprehensive guide to the SparkDataMesh Data Contract Domain Specific Language (DSL) for defining data schemas, transformations, and quality rules.

## Table of Contents

1. [Overview](#overview)
2. [Contract Structure](#contract-structure)
3. [Data Types](#data-types)
4. [Table Definitions](#table-definitions)
5. [Transformation Operations](#transformation-operations)
6. [Quality Validations](#quality-validations)
7. [Advanced Features](#advanced-features)
8. [Complete Examples](#complete-examples)
9. [Best Practices](#best-practices)

## Overview

The Data Contract DSL allows you to define:
- **Schema definitions** for source and target tables
- **Transformation rules** for data processing
- **Quality validations** for data integrity
- **Optimization hints** for performance

## Contract Structure

Every data contract follows this basic structure:

```yaml
dataContractSpecification: "1.0.0"
id: "unique-contract-id"
info:
  title: "Contract Title"
  version: "1.0.0"
  description: "Contract description"
  owner: "Data Team"
  contact:
    name: "Contact Name"
    email: "contact@company.com"

servers:
  - type: "spark"
    environment: "production"
    description: "Production Spark cluster"
    connectionString: "database:my_db"

schema:
  tables:
    # Table definitions

transformations:
  # Transformation rules

quality:
  expectations:
    # Quality validations
```

## Data Types

### Primitive Types

| Type | Description | Example | Spark SQL Type |
|------|-------------|---------|----------------|
| `string` | Text data | "John Doe" | STRING |
| `integer` | 32-bit integers | 42 | INT |
| `long` | 64-bit integers | 9223372036854775807 | BIGINT |
| `double` | Double precision floating point | 3.14159 | DOUBLE |
| `boolean` | True/false values | true | BOOLEAN |
| `timestamp` | Date and time | "2024-01-15T10:30:00Z" | TIMESTAMP |
| `date` | Date only | "2024-01-15" | DATE |
| `decimal` | Fixed precision decimal | 123.45 | DECIMAL(p,s) |

### Complex Types

| Type | Description | Example Usage |
|------|-------------|---------------|
| `array` | List of values | Array of strings or numbers |
| `struct` | Nested object | Complex nested data structures |

### Field Type Examples

```yaml
fields:
  # String field with constraints
  user_name:
    type: "string"
    description: "User's full name"
    required: true
    nullable: false
    constraints:
      minLength: 2
      maxLength: 100
      pattern: "^[A-Za-z\\s]+$"
  
  # Integer field with range
  age:
    type: "integer"
    description: "User age"
    required: true
    constraints:
      minimum: 0
      maximum: 150
  
  # Decimal field with precision
  price:
    type: "decimal"
    description: "Product price"
    precision: 10
    scale: 2
    constraints:
      minimum: 0
  
  # Enum field
  status:
    type: "string"
    description: "Order status"
    required: true
    constraints:
      enum: ["pending", "processing", "shipped", "delivered", "cancelled"]
  
  # Timestamp field
  created_at:
    type: "timestamp"
    description: "Record creation time"
    required: true
    nullable: false
  
  # Optional field
  optional_field:
    type: "string"
    description: "Optional metadata"
    required: false
    nullable: true
```

## Table Definitions

### Table Types

#### Source Tables
Input tables from source systems:

```yaml
raw_orders:
  type: "source"
  domain: "order"
  description: "Raw order data from e-commerce system"
  physicalName: "orders_raw"
  fields:
    order_id:
      type: "string"
      description: "Unique order identifier"
      required: true
      primaryKey: true
    customer_id:
      type: "string"
      description: "Customer identifier"
      required: true
    order_date:
      type: "timestamp"
      description: "Order placement timestamp"
      required: true
    total_amount:
      type: "decimal"
      precision: 10
      scale: 2
      description: "Total order amount"
  partitionBy: ["order_date"]
  clusterBy: ["customer_id"]
```

#### Target Tables
Output tables after transformation:

```yaml
processed_orders:
  type: "target"
  domain: "order"
  description: "Processed and enriched order data"
  physicalName: "orders_processed"
  fields:
    order_id:
      type: "string"
      description: "Unique order identifier"
      required: true
      primaryKey: true
    customer_id:
      type: "string"
      description: "Customer identifier"
      required: true
    order_date:
      type: "timestamp"
      description: "Order placement timestamp"
      required: true
    total_amount_usd:
      type: "decimal"
      precision: 12
      scale: 2
      description: "Total amount in USD"
    is_high_value:
      type: "boolean"
      description: "Flag for high-value orders"
    risk_category:
      type: "string"
      description: "Risk assessment category"
      constraints:
        enum: ["low", "medium", "high"]
  partitionBy: ["order_date"]
```

#### Intermediate Tables
Temporary processing tables:

```yaml
order_aggregates:
  type: "intermediate"
  domain: "order"
  description: "Customer order aggregations"
  fields:
    customer_id:
      type: "string"
      required: true
      primaryKey: true
    total_orders:
      type: "integer"
      description: "Total number of orders"
    total_spent:
      type: "decimal"
      precision: 12
      scale: 2
      description: "Total amount spent"
    avg_order_value:
      type: "decimal"
      precision: 10
      scale: 2
      description: "Average order value"
```

### Domain Classifications

Supported domains for organizing data:

```yaml
# Transaction domain
domain: "transaction"  # Financial transactions, payments

# Order domain  
domain: "order"        # E-commerce orders, fulfillment

# User domain
domain: "user"         # User profiles, authentication

# Product domain
domain: "product"      # Product catalog, inventory

# Ads domain
domain: "ads"          # Advertising campaigns, clicks

# Search domain
domain: "search"       # Search queries, results
```

## Transformation Operations

### Transformation Types

#### Transform Operations
Field-level data transformations:

```yaml
transformations:
  order_enrichment:
    type: "transform"
    description: "Enrich orders with calculated fields"
    source: "raw_orders"
    target: "processed_orders"
    rules:
      # Direct field mapping
      - field: "order_id"
        operation: "map"
        expression: "order_id"
      
      # Currency conversion
      - field: "total_amount_usd"
        operation: "map"
        expression: "CASE WHEN currency = 'USD' THEN total_amount ELSE total_amount * exchange_rate END"
      
      # Boolean calculation
      - field: "is_high_value"
        operation: "map"
        expression: "CASE WHEN total_amount >= 1000 THEN true ELSE false END"
      
      # String manipulation
      - field: "customer_tier"
        operation: "map"
        expression: "UPPER(SUBSTRING(customer_type, 1, 3))"
      
      # Conditional logic
      - field: "risk_category"
        operation: "map"
        expression: "CASE WHEN total_amount >= 5000 THEN 'high' WHEN total_amount >= 1000 THEN 'medium' ELSE 'low' END"
```

#### Aggregate Operations
Data aggregation and grouping:

```yaml
transformations:
  customer_summary:
    type: "aggregate"
    description: "Customer order summaries"
    source: "raw_orders"
    target: "customer_aggregates"
    rules:
      - field: "customer_id"
        operation: "map"
        expression: "customer_id"
      
      - field: "total_orders"
        operation: "aggregate"
        expression: "COUNT(*)"
      
      - field: "total_spent"
        operation: "aggregate"
        expression: "SUM(total_amount)"
      
      - field: "avg_order_value"
        operation: "aggregate"
        expression: "AVG(total_amount)"
      
      - field: "last_order_date"
        operation: "aggregate"
        expression: "MAX(order_date)"
```

#### Join Operations
Combining multiple data sources:

```yaml
transformations:
  order_customer_join:
    type: "join"
    description: "Join orders with customer data"
    source: ["raw_orders", "customer_profiles"]
    target: "enriched_orders"
    rules:
      # Fields from orders table
      - field: "order_id"
        operation: "map"
        expression: "o.order_id"
      
      - field: "order_date"
        operation: "map"
        expression: "o.order_date"
      
      # Fields from customer table
      - field: "customer_name"
        operation: "map"
        expression: "c.full_name"
      
      - field: "customer_segment"
        operation: "map"
        expression: "c.segment"
    sql: |
      SELECT 
        o.order_id,
        o.order_date,
        o.total_amount,
        c.full_name as customer_name,
        c.segment as customer_segment
      FROM raw_orders o
      JOIN customer_profiles c ON o.customer_id = c.customer_id
```

#### Filter Operations
Row-level filtering:

```yaml
transformations:
  high_value_orders:
    type: "filter"
    description: "Filter for high-value orders only"
    source: "raw_orders"
    target: "high_value_orders"
    rules:
      - field: "*"
        operation: "filter"
        condition: "total_amount >= 1000 AND status = 'completed'"
```

### Field-Level Operations

| Operation | Description | Example Expression |
|-----------|-------------|-------------------|
| `map` | Direct field mapping | `field_name` |
| `cast` | Type conversion | `CAST(amount AS DECIMAL(10,2))` |
| `concat` | String concatenation | `CONCAT(first_name, ' ', last_name)` |
| `split` | String splitting | `SPLIT(full_name, ' ')[0]` |
| `extract` | Value extraction | `EXTRACT(YEAR FROM order_date)` |
| `aggregate` | Aggregation functions | `SUM(amount)`, `COUNT(*)`, `AVG(price)` |
| `filter` | Conditional filtering | `amount > 100 AND status = 'active'` |

### Custom SQL Transformations

For complex logic, use custom SQL:

```yaml
transformations:
  complex_transformation:
    type: "transform"
    description: "Complex business logic transformation"
    source: "raw_data"
    target: "processed_data"
    sql: |
      SELECT 
        transaction_id,
        user_id,
        amount,
        CASE 
          WHEN amount >= 10000 THEN 'enterprise'
          WHEN amount >= 1000 THEN 'premium'
          ELSE 'standard'
        END as customer_tier,
        LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY transaction_date) as prev_amount,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_date DESC) as recency_rank
      FROM raw_data
      WHERE transaction_date >= CURRENT_DATE - INTERVAL 30 DAYS
```

## Quality Validations

### Validation Types

#### Not Null Validation
Ensure required fields are populated:

```yaml
quality:
  expectations:
    - type: "not_null"
      table: "raw_orders"
      field: "order_id"
      description: "Order ID must never be null"
      severity: "error"
    
    - type: "not_null"
      table: "raw_orders"
      field: "customer_id"
      description: "Customer ID is required"
      severity: "error"
```

#### Uniqueness Validation
Ensure field values are unique:

```yaml
quality:
  expectations:
    - type: "unique"
      table: "raw_orders"
      field: "order_id"
      description: "Order IDs must be unique"
      severity: "error"
```

#### Range Validation
Validate numeric ranges:

```yaml
quality:
  expectations:
    - type: "range"
      table: "raw_orders"
      field: "total_amount"
      description: "Order amount should be positive and reasonable"
      expression: "0,1000000"  # min,max
      threshold: 5.0           # Allow 5% violation rate
      severity: "warning"
```

#### Pattern Validation
Validate string patterns:

```yaml
quality:
  expectations:
    - type: "pattern"
      table: "raw_orders"
      field: "order_id"
      description: "Order ID should follow format ORD-YYYYMMDD-NNNN"
      expression: "^ORD-\\d{8}-\\d{4}$"
      threshold: 1.0           # 100% compliance required
      severity: "error"
    
    - type: "pattern"
      table: "customer_profiles"
      field: "email"
      description: "Email should be valid format"
      expression: "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
      threshold: 2.0           # Allow 2% invalid emails
      severity: "warning"
```

#### Completeness Validation
Check data completeness:

```yaml
quality:
  expectations:
    - type: "completeness"
      table: "processed_orders"
      field: "total_amount_usd"
      description: "USD amount should be calculated for all orders"
      threshold: 100.0         # 100% completeness required
      severity: "error"
```

#### Freshness Validation
Validate data recency:

```yaml
quality:
  expectations:
    - type: "freshness"
      table: "raw_orders"
      field: "order_date"
      description: "Order data should be fresh within 24 hours"
      expression: "24"         # Hours
      threshold: 95.0          # 95% of data should be fresh
      severity: "warning"
```

#### Custom Validation
Custom business logic validation:

```yaml
quality:
  expectations:
    - type: "custom"
      table: "processed_orders"
      description: "High-value orders should have risk assessment"
      expression: "CASE WHEN total_amount >= 1000 AND risk_category IS NULL THEN 0 ELSE 1 END"
      threshold: 100.0
      severity: "error"
```

## Advanced Features

### Partitioning and Clustering

Optimize table performance:

```yaml
tables:
  large_transaction_table:
    type: "source"
    fields:
      # ... field definitions
    partitionBy: ["transaction_date", "region"]  # Partition by date and region
    clusterBy: ["customer_id", "transaction_type"]  # Cluster by customer and type
```

### Field Constraints

Comprehensive field validation:

```yaml
fields:
  user_age:
    type: "integer"
    constraints:
      minimum: 13      # Minimum age requirement
      maximum: 120     # Maximum reasonable age
  
  product_code:
    type: "string"
    constraints:
      minLength: 5     # Minimum length
      maxLength: 20    # Maximum length
      pattern: "^[A-Z]{2}\\d{3,18}$"  # Format validation
  
  order_status:
    type: "string"
    constraints:
      enum: ["pending", "confirmed", "shipped", "delivered", "cancelled"]
  
  email:
    type: "string"
    format: "email"    # Built-in email validation
```

### Multi-Source Transformations

Join multiple source tables:

```yaml
transformations:
  order_analytics:
    type: "join"
    description: "Comprehensive order analytics"
    source: ["orders", "customers", "products", "regions"]
    target: "order_analytics_fact"
    sql: |
      SELECT 
        o.order_id,
        o.order_date,
        c.customer_segment,
        p.category as product_category,
        r.region_name,
        o.quantity * p.unit_price as revenue,
        CASE 
          WHEN c.customer_segment = 'premium' AND o.quantity * p.unit_price > 1000 THEN 'high_value_premium'
          WHEN c.customer_segment = 'premium' THEN 'premium'
          WHEN o.quantity * p.unit_price > 1000 THEN 'high_value_standard'
          ELSE 'standard'
        END as order_classification
      FROM orders o
      JOIN customers c ON o.customer_id = c.customer_id
      JOIN products p ON o.product_id = p.product_id
      JOIN regions r ON c.region_id = r.region_id
      WHERE o.order_date >= CURRENT_DATE - INTERVAL 365 DAYS
```

## Complete Examples

### E-commerce Order Processing

```yaml
dataContractSpecification: "1.0.0"
id: "ecommerce-order-processing"
info:
  title: "E-commerce Order Processing Contract"
  version: "2.1.0"
  description: "Complete order processing pipeline from raw orders to analytics"
  owner: "E-commerce Data Team"
  contact:
    name: "Data Engineering"
    email: "data-eng@ecommerce.com"

servers:
  - type: "spark"
    environment: "production"
    description: "Production Spark cluster for order processing"
    connectionString: "database:ecommerce_prod"

schema:
  tables:
    # Source table: Raw orders from application
    raw_orders:
      type: "source"
      domain: "order"
      description: "Raw order data from e-commerce application"
      physicalName: "app_orders_raw"
      fields:
        order_id:
          type: "string"
          description: "Unique order identifier"
          required: true
          primaryKey: true
          constraints:
            pattern: "^ORD-\\d{8}-\\d{6}$"
        customer_id:
          type: "string"
          description: "Customer identifier"
          required: true
          nullable: false
        order_date:
          type: "timestamp"
          description: "Order placement timestamp"
          required: true
        items:
          type: "array"
          description: "Array of order items"
        total_amount:
          type: "decimal"
          precision: 10
          scale: 2
          description: "Total order amount in local currency"
          constraints:
            minimum: 0
        currency:
          type: "string"
          description: "Order currency code"
          required: true
          constraints:
            pattern: "^[A-Z]{3}$"
        status:
          type: "string"
          description: "Current order status"
          required: true
          constraints:
            enum: ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled"]
        shipping_address:
          type: "struct"
          description: "Shipping address information"
        payment_method:
          type: "string"
          description: "Payment method used"
      partitionBy: ["order_date"]
      clusterBy: ["customer_id", "status"]

    # Target table: Processed orders
    processed_orders:
      type: "target"
      domain: "order"
      description: "Processed and enriched order data for analytics"
      physicalName: "orders_processed_v2"
      fields:
        order_id:
          type: "string"
          description: "Unique order identifier"
          required: true
          primaryKey: true
        customer_id:
          type: "string"
          description: "Customer identifier"
          required: true
        order_date:
          type: "timestamp"
          description: "Order placement timestamp"
          required: true
        item_count:
          type: "integer"
          description: "Number of items in order"
          required: true
        total_amount_usd:
          type: "decimal"
          precision: 12
          scale: 2
          description: "Total order amount in USD"
          required: true
        original_amount:
          type: "decimal"
          precision: 10
          scale: 2
          description: "Original amount in local currency"
        original_currency:
          type: "string"
          description: "Original currency code"
        status:
          type: "string"
          description: "Current order status"
          required: true
        is_high_value:
          type: "boolean"
          description: "Flag for high-value orders (>= $500)"
          required: true
        customer_tier:
          type: "string"
          description: "Customer tier based on order value"
          constraints:
            enum: ["bronze", "silver", "gold", "platinum"]
        risk_score:
          type: "double"
          description: "Fraud risk score (0.0 - 1.0)"
        shipping_country:
          type: "string"
          description: "Shipping country code"
        payment_category:
          type: "string"
          description: "Categorized payment method"
        processing_date:
          type: "timestamp"
          description: "When this record was processed"
          required: true
      partitionBy: ["order_date"]
      clusterBy: ["customer_tier", "shipping_country"]

    # Intermediate table: Customer aggregations
    customer_order_summary:
      type: "intermediate"
      domain: "order"
      description: "Customer order aggregations for tier calculation"
      fields:
        customer_id:
          type: "string"
          required: true
          primaryKey: true
        total_orders:
          type: "integer"
          description: "Total number of orders"
        total_spent_usd:
          type: "decimal"
          precision: 12
          scale: 2
          description: "Total amount spent in USD"
        avg_order_value:
          type: "decimal"
          precision: 10
          scale: 2
          description: "Average order value"
        last_order_date:
          type: "timestamp"
          description: "Date of most recent order"
        order_frequency_days:
          type: "double"
          description: "Average days between orders"

transformations:
  # Step 1: Basic order enrichment
  order_enrichment:
    type: "transform"
    description: "Enrich orders with calculated fields and currency conversion"
    source: "raw_orders"
    target: "processed_orders"
    rules:
      # Direct mappings
      - field: "order_id"
        operation: "map"
        expression: "order_id"
      
      - field: "customer_id"
        operation: "map"
        expression: "customer_id"
      
      - field: "order_date"
        operation: "map"
        expression: "order_date"
      
      - field: "status"
        operation: "map"
        expression: "status"
      
      - field: "original_amount"
        operation: "map"
        expression: "total_amount"
      
      - field: "original_currency"
        operation: "map"
        expression: "currency"
      
      # Calculated fields
      - field: "item_count"
        operation: "map"
        expression: "SIZE(items)"
      
      # Currency conversion (simplified - in reality would use exchange rate table)
      - field: "total_amount_usd"
        operation: "map"
        expression: |
          CASE 
            WHEN currency = 'USD' THEN total_amount
            WHEN currency = 'EUR' THEN total_amount * 1.1
            WHEN currency = 'GBP' THEN total_amount * 1.25
            WHEN currency = 'CAD' THEN total_amount * 0.75
            ELSE total_amount  -- Default to original if unknown currency
          END
      
      # Business logic flags
      - field: "is_high_value"
        operation: "map"
        expression: "total_amount_usd >= 500"
      
      # Customer tier calculation
      - field: "customer_tier"
        operation: "map"
        expression: |
          CASE 
            WHEN total_amount_usd >= 2000 THEN 'platinum'
            WHEN total_amount_usd >= 1000 THEN 'gold'
            WHEN total_amount_usd >= 300 THEN 'silver'
            ELSE 'bronze'
          END
      
      # Risk scoring (simplified)
      - field: "risk_score"
        operation: "map"
        expression: |
          CASE 
            WHEN total_amount_usd >= 5000 THEN 0.8
            WHEN total_amount_usd >= 2000 THEN 0.5
            WHEN payment_method = 'credit_card' THEN 0.2
            ELSE 0.1
          END
      
      # Address extraction
      - field: "shipping_country"
        operation: "map"
        expression: "shipping_address.country"
      
      # Payment categorization
      - field: "payment_category"
        operation: "map"
        expression: |
          CASE 
            WHEN payment_method IN ('visa', 'mastercard', 'amex') THEN 'credit_card'
            WHEN payment_method = 'paypal' THEN 'digital_wallet'
            WHEN payment_method = 'bank_transfer' THEN 'bank'
            ELSE 'other'
          END
      
      - field: "processing_date"
        operation: "map"
        expression: "CURRENT_TIMESTAMP()"

  # Step 2: Customer aggregations
  customer_aggregation:
    type: "aggregate"
    description: "Calculate customer-level order statistics"
    source: "processed_orders"
    target: "customer_order_summary"
    sql: |
      SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(total_amount_usd) as total_spent_usd,
        AVG(total_amount_usd) as avg_order_value,
        MAX(order_date) as last_order_date,
        CASE 
          WHEN COUNT(*) > 1 
          THEN DATEDIFF(MAX(order_date), MIN(order_date)) / (COUNT(*) - 1)
          ELSE NULL 
        END as order_frequency_days
      FROM processed_orders
      WHERE status NOT IN ('cancelled')
      GROUP BY customer_id

quality:
  expectations:
    # Data quality for source tables
    - type: "not_null"
      table: "raw_orders"
      field: "order_id"
      description: "Order ID is required for all records"
      severity: "error"
    
    - type: "not_null"
      table: "raw_orders"
      field: "customer_id"
      description: "Customer ID is required for all orders"
      severity: "error"
    
    - type: "unique"
      table: "raw_orders"
      field: "order_id"
      description: "Order IDs must be unique"
      severity: "error"
    
    - type: "pattern"
      table: "raw_orders"
      field: "order_id"
      description: "Order ID should follow correct format"
      expression: "^ORD-\\d{8}-\\d{6}$"
      threshold: 1.0
      severity: "error"
    
    - type: "range"
      table: "raw_orders"
      field: "total_amount"
      description: "Order amount should be positive and reasonable"
      expression: "0,100000"
      threshold: 2.0  # Allow 2% outliers
      severity: "warning"
    
    - type: "pattern"
      table: "raw_orders"
      field: "currency"
      description: "Currency should be valid 3-letter code"
      expression: "^[A-Z]{3}$"
      threshold: 1.0
      severity: "error"
    
    # Data quality for processed tables
    - type: "completeness"
      table: "processed_orders"
      field: "total_amount_usd"
      description: "USD amount should be calculated for all orders"
      threshold: 100.0
      severity: "error"
    
    - type: "completeness"
      table: "processed_orders"
      field: "customer_tier"
      description: "Customer tier should be assigned to all orders"
      threshold: 100.0
      severity: "error"
    
    - type: "range"
      table: "processed_orders"
      field: "risk_score"
      description: "Risk score should be between 0 and 1"
      expression: "0,1"
      threshold: 1.0
      severity: "error"
    
    # Business logic validations
    - type: "custom"
      table: "processed_orders"
      description: "High-value orders should have appropriate risk scores"
      expression: "CASE WHEN is_high_value = true AND risk_score < 0.3 THEN 0 ELSE 1 END"
      threshold: 90.0  # Allow 10% exceptions
      severity: "warning"
    
    - type: "custom"
      table: "processed_orders"
      description: "Platinum tier customers should have high order values"
      expression: "CASE WHEN customer_tier = 'platinum' AND total_amount_usd < 1000 THEN 0 ELSE 1 END"
      threshold: 95.0
      severity: "warning"
    
    # Data freshness
    - type: "freshness"
      table: "raw_orders"
      field: "order_date"
      description: "Order data should be fresh within 4 hours"
      expression: "4"
      threshold: 95.0
      severity: "warning"
```

## Best Practices

### 1. Schema Design
- **Use descriptive field names** that clearly indicate the data content
- **Include comprehensive descriptions** for all fields and tables
- **Define appropriate constraints** to ensure data quality
- **Use proper data types** with correct precision for decimals

### 2. Transformation Logic
- **Keep transformations simple** and well-documented
- **Use meaningful transformation names** that describe the business purpose
- **Include comments** in complex SQL expressions
- **Test edge cases** in transformation logic

### 3. Quality Validations
- **Implement multiple validation layers** (not_null, unique, range, pattern)
- **Set appropriate thresholds** based on business requirements
- **Use appropriate severity levels** (error for critical, warning for informational)
- **Monitor quality metrics** regularly

### 4. Performance Optimization
- **Choose partition columns** based on query patterns
- **Use clustering** for frequently filtered columns
- **Consider data skew** when designing partitioning strategy
- **Test with realistic data volumes**

### 5. Documentation
- **Keep contracts up-to-date** with schema changes
- **Version contracts** appropriately
- **Include business context** in descriptions
- **Document data lineage** and dependencies

### 6. Testing
- **Validate contracts** before deployment
- **Test with sample data** to verify transformations
- **Monitor quality metrics** in production
- **Set up alerting** for quality failures

This guide provides a comprehensive foundation for using the SparkDataMesh Data Contract DSL to define robust, well-documented data processing pipelines.