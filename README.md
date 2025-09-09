# SparkDataFlow

> A contract-driven data processing framework that transforms domain data using Apache Spark with automated SQL generation and comprehensive data quality validation.

## 🚀 Introduction

SparkDataFlow is a modern data engineering framework that revolutionizes how you build and manage data pipelines. By leveraging the **Open Data Contract Standard (ODCS)**, it provides a declarative approach to data processing that eliminates boilerplate code and ensures data quality.

### Why SparkDataFlow?

**🎯 Contract-First Development**
- Define your data transformations using standardized YAML contracts
- Automatically generate optimized Spark SQL from high-level specifications
- Ensure consistency across teams with shared contract standards

**🔍 Built-in Data Quality**
- Comprehensive validation framework with 7+ built-in validators
- Real-time data quality monitoring with configurable severity levels
- Fail-fast approach prevents bad data from propagating downstream

**🏗️ Domain-Driven Architecture**
- Organize data processing by business domains (transactions, orders, ads, search)
- Pluggable data source adapters for various storage formats
- Clean separation of concerns between data access, transformation, and validation

**⚡ Performance Optimized**
- Leverages Spark's adaptive query execution and catalyst optimizer
- Intelligent caching and resource management
- Support for partitioning and clustering strategies

### What SparkDataFlow Does

SparkDataFlow enables you to:
- 📊 **Read** domain data from Spark tables, Delta Lake, Parquet, JSON, CSV sources
- 📝 **Define** transformations using declarative YAML contracts instead of code
- 🔍 **Validate** data quality with comprehensive expectation rules
- 🔄 **Transform** data using auto-generated, optimized Spark SQL
- 💾 **Sink** processed data to target tables with partitioning support
- 📈 **Monitor** pipeline execution with detailed logging and metrics

## Features

### ✅ Data Contract Abstraction
- YAML/JSON contract definitions based on Open Data Contract Standard
- Schema validation with JSON Schema
- Domain-specific data source encapsulation

### ✅ Multi-Source Support
- Spark/Hive tables
- Delta Lake tables
- Parquet files
- JSON files
- CSV files

### ✅ SQL Generation Engine
- Auto-generates Spark SQL from transformation rules
- Supports extract, transform, aggregate, join, filter, and union operations
- Custom SQL template support with variable substitution

### ✅ Data Validation Pipeline
- Comprehensive data quality expectations
- Built-in validators: not_null, unique, range, pattern, freshness, completeness
- Custom validation expressions
- Configurable severity levels (error, warning, info)

### ✅ Target Table Sinks
- Multiple output formats supported
- Partitioning and clustering support
- Configurable write modes

## Quick Start

### 1. Add Dependencies

The project uses Maven with the following key dependencies:
- Apache Spark 3.5.0
- Jackson for JSON/YAML processing
- JSON Schema validation
- SLF4J logging

### 2. Create a Data Contract

```yaml
dataContractSpecification: "1.0.0"
id: "transaction-processing"
info:
  title: "Transaction Data Processing"
  version: "1.0.0"
  description: "Process raw transactions with validation and enrichment"

servers:
  - type: "spark"
    environment: "production"
    connectionString: "database:transaction_db"

schema:
  tables:
    raw_transactions:
      type: "source"
      domain: "transaction"
      fields:
        transaction_id:
          type: "string"
          required: true
          primaryKey: true
        amount:
          type: "decimal"
          precision: 10
          scale: 2
        currency:
          type: "string"
          constraints:
            pattern: "^[A-Z]{3}$"
        transaction_date:
          type: "timestamp"
      partitionBy: ["transaction_date"]

transformations:
  currency_conversion:
    type: "transform"
    source: "raw_transactions"
    target: "processed_transactions"
    rules:
      - field: "amount_usd"
        operation: "map"
        expression: "CASE WHEN currency = 'USD' THEN amount ELSE amount * 1.0 END"

quality:
  expectations:
    - type: "not_null"
      table: "raw_transactions"
      field: "transaction_id"
      severity: "error"
```

### 3. Run the Application

```bash
java -cp target/spark-data-flow-1.0-SNAPSHOT.jar \
  io.sparkdataflow.core.SparkDataFlowApplication \
  src/main/resources/contracts/sample-transaction-contract.yaml \
  src/main/resources/config/application.conf
```

## 🏗️ Architecture

SparkDataFlow follows a **layered, domain-driven architecture** that promotes separation of concerns and extensibility.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Contract Layer                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  Contract YAML  │  │  JSON Schema    │  │  Contract POJO  │ │
│  │  Definitions    │  │  Validation     │  │  Models         │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Execution Engine Layer                       │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  SQL Generator  │  │  Data Validator │  │  DataFlow       │ │
│  │  Engine         │  │  Framework      │  │  Engine         │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Access Layer                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  Domain Data    │  │  Data Source    │  │  Data Sink      │ │
│  │  Sources        │  │  Factory        │  │  Adapters       │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Storage Layer                                │
│     Spark Tables  │  Delta Lake  │  Parquet  │  JSON  │  CSV    │
└─────────────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. **Data Contract Layer**
- **`DataContract`**: POJO representation with JSON Schema validation
- **`DataContractLoader`**: Loads and validates contracts from YAML/JSON
- **Contract Schema**: JSON Schema defining contract structure and validation rules

#### 2. **Execution Engine Layer**
- **`DataFlowEngine`**: Orchestrates the entire pipeline execution flow
- **`SQLGenerator`**: Converts transformation rules into optimized Spark SQL
- **`DataValidator`**: Executes data quality validations with configurable thresholds

#### 3. **Data Access Layer**
- **`DomainDataSource`**: Abstract interface for domain-specific data access
- **`DomainDataSourceFactory`**: Creates appropriate data source instances
- **`DataSink`**: Handles writing to various target storage formats

#### 4. **Storage Layer**
- Pluggable adapters for different storage formats
- Optimized readers and writers with caching support
- Connection pooling and resource management

### Domain Data Sources

- `SparkTableDataSource`: For Spark/Hive tables
- `DeltaTableDataSource`: For Delta Lake tables
- `ParquetDataSource`: For Parquet files
- `JsonDataSource`: For JSON files
- `CsvDataSource`: For CSV files

### Transformation Types

- **extract**: Select specific columns with expressions
- **transform**: Apply field-level transformations
- **aggregate**: Group and aggregate data
- **join**: Join multiple source tables
- **filter**: Apply filter conditions
- **union**: Union multiple tables

### Validation Types

- **not_null**: Check for null values
- **unique**: Check for duplicate values
- **range**: Validate numeric ranges
- **pattern**: Validate string patterns (regex)
- **freshness**: Check data recency
- **completeness**: Check data completeness percentage
- **custom**: Custom SQL validation expressions

## 📋 Getting Started Guide

### Prerequisites

- Java 17 or higher
- Apache Maven 3.6+
- Apache Spark 3.5.0 (included in dependencies)

### Step 1: Clone and Build

```bash
git clone <repository-url>
cd SparkDataFlow
mvn clean package
```

### Step 2: Understand the Data Contract Structure

Data contracts define the complete data processing pipeline:

```yaml
# Contract Metadata
dataContractSpecification: "1.0.0"
id: "my-data-pipeline"
info:
  title: "My Data Pipeline"
  version: "1.0.0"

# Data Sources Configuration
servers:
  - type: "spark"                    # Source type
    environment: "production"         # Environment
    connectionString: "database:db"   # Connection details

# Schema Definitions
schema:
  tables:
    source_table:
      type: "source"                  # source | target | intermediate
      domain: "transaction"           # Business domain
      fields:                         # Field definitions with types and constraints
        field_name:
          type: "string"
          required: true

# Transformation Rules
transformations:
  my_transformation:
    type: "transform"                 # extract | transform | aggregate | join | filter | union
    source: "source_table"
    target: "target_table"
    rules:                           # Field-level transformation rules
      - field: "new_field"
        operation: "map"
        expression: "UPPER(old_field)"

# Data Quality Validations
quality:
  expectations:
    - type: "not_null"               # Validation type
      table: "source_table"
      field: "required_field"
      severity: "error"              # error | warning | info
```

### Step 3: Create Your First Pipeline

1. **Define your data contract** (`my-pipeline.yaml`):

```yaml
dataContractSpecification: "1.0.0"
id: "user-transaction-enrichment"
info:
  title: "User Transaction Enrichment Pipeline"
  version: "1.0.0"
  description: "Enrich user transactions with risk scoring"

servers:
  - type: "spark"
    environment: "development"
    connectionString: "database:analytics_db"

schema:
  tables:
    raw_user_transactions:
      type: "source"
      domain: "transaction"
      physicalName: "raw_transactions"
      fields:
        user_id:
          type: "string"
          required: true
        amount:
          type: "decimal"
          precision: 10
          scale: 2
        transaction_date:
          type: "timestamp"
      partitionBy: ["transaction_date"]
    
    enriched_transactions:
      type: "target"
      domain: "transaction"
      fields:
        user_id:
          type: "string"
        amount:
          type: "decimal"
        risk_level:
          type: "string"
        is_high_value:
          type: "boolean"

transformations:
  enrich_transactions:
    type: "transform"
    source: "raw_user_transactions"
    target: "enriched_transactions"
    rules:
      - field: "user_id"
        operation: "map"
        expression: "user_id"
      - field: "amount"
        operation: "map"
        expression: "amount"
      - field: "risk_level"
        operation: "map"
        expression: "CASE WHEN amount > 1000 THEN 'HIGH' WHEN amount > 100 THEN 'MEDIUM' ELSE 'LOW' END"
      - field: "is_high_value"
        operation: "map"
        expression: "amount > 1000"

quality:
  expectations:
    - type: "not_null"
      table: "raw_user_transactions"
      field: "user_id"
      severity: "error"
    - type: "range"
      table: "raw_user_transactions"
      field: "amount"
      expression: "0,100000"
      threshold: 5.0
      severity: "warning"
```

2. **Run the pipeline**:

```bash
java -cp target/spark-data-flow-1.0-SNAPSHOT.jar \
  io.sparkdataflow.core.SparkDataFlowApplication \
  my-pipeline.yaml
```

### Step 4: Monitor and Debug

SparkDataFlow provides comprehensive logging:

```
2024-01-15 10:30:15 INFO  DataFlowEngine - Executing data contract: user-transaction-enrichment
2024-01-15 10:30:16 INFO  SparkTableDataSource - Reading Spark table: analytics_db.raw_transactions
2024-01-15 10:30:18 INFO  SQLGenerator - Generated SQL: SELECT user_id, amount, CASE WHEN...
2024-01-15 10:30:20 INFO  DataValidator - Validating expectation: not_null on table: raw_user_transactions
2024-01-15 10:30:21 INFO  DataSink - Successfully sunk target table: enriched_transactions
```

## ⚙️ Configuration Guide

### Application Configuration (application.conf)

```hocon
spark {
  appName = "SparkDataFlow"
  master = "local[*]"                    # Spark master URL
  
  configs {
    # Performance optimizations
    "spark.sql.adaptive.enabled" = "true"
    "spark.sql.adaptive.coalescePartitions.enabled" = "true"
    "spark.sql.adaptive.skewJoin.enabled" = "true"
    
    # Serialization
    "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
    
    # Storage
    "spark.sql.warehouse.dir" = "/tmp/spark-warehouse"
    "spark.sql.parquet.compression.codec" = "snappy"
  }
}

data {
  defaultPath = "/tmp/sparkdataflow"      # Default data path
}

validation {
  enabled = true                          # Enable/disable data validation
}

execution {
  maxRetries = 3                          # Maximum retry attempts
  timeoutMs = 300000                      # Execution timeout (5 minutes)
}
```

### Environment-Specific Configurations

**Development** (`application-dev.conf`):
```hocon
spark.master = "local[2]"
validation.enabled = true
logging.level = "DEBUG"
```

**Production** (`application-prod.conf`):
```hocon
spark.master = "yarn"
spark.configs {
  "spark.executor.memory" = "4g"
  "spark.executor.cores" = "4"
  "spark.sql.adaptive.enabled" = "true"
}
validation.enabled = true
```

## Examples

See `src/main/resources/contracts/sample-transaction-contract.yaml` for a complete example that demonstrates:
- Reading raw transaction data
- Currency conversion and risk scoring
- Comprehensive data quality validations
- Writing to partitioned target tables

## Project Structure

```
src/
├── main/
│   ├── java/io/sparkdataflow/core/
│   │   ├── contract/          # Data contract models and loader
│   │   ├── domain/            # Domain data source implementations
│   │   ├── engine/            # SQL generation and execution engine
│   │   ├── validation/        # Data validation framework
│   │   ├── sink/              # Data sink implementations
│   │   ├── config/            # Application configuration
│   │   └── SparkDataFlowApplication.java
│   └── resources/
│       ├── schemas/           # JSON schema definitions
│       ├── contracts/         # Sample data contracts
│       └── config/            # Configuration files
└── test/                      # Test files
```

## 📖 Advanced Usage Patterns

### Multi-Domain Processing

Process multiple business domains in a single pipeline:

```yaml
schema:
  tables:
    # Transaction domain
    raw_transactions:
      domain: "transaction"
      type: "source"
    
    # Order domain  
    raw_orders:
      domain: "order"
      type: "source"
    
    # Joined output
    transaction_orders:
      domain: "analytics"
      type: "target"

transformations:
  join_domains:
    type: "join"
    source: ["raw_transactions", "raw_orders"]
    target: "transaction_orders"
    rules:
      - condition: "raw_transactions.order_id = raw_orders.id"
```

### Custom SQL Transformations

Use custom SQL for complex transformations:

```yaml
transformations:
  complex_analytics:
    type: "transform"
    source: "raw_events"
    target: "user_segments"
    sql: |
      SELECT 
        user_id,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount,
        COUNT(*) as transaction_count,
        CASE 
          WHEN COUNT(*) > 100 THEN 'power_user'
          WHEN COUNT(*) > 10 THEN 'regular_user'
          ELSE 'casual_user'
        END as user_segment
      FROM ${raw_events}
      WHERE transaction_date >= current_date() - interval 30 days
      GROUP BY user_id
```

### Advanced Data Validation

```yaml
quality:
  expectations:
    # Custom business rule validation
    - type: "custom"
      table: "transactions"
      description: "Revenue reconciliation check"
      expression: "SUM(amount) BETWEEN daily_target * 0.8 AND daily_target * 1.2"
      severity: "error"
    
    # Data freshness validation
    - type: "freshness"
      table: "events"
      field: "created_at"
      expression: "2"  # 2 hours
      threshold: 95.0
      severity: "warning"
    
    # Cross-table consistency
    - type: "custom"
      table: "order_summary"
      expression: "total_orders = (SELECT COUNT(*) FROM raw_orders WHERE status = 'completed')"
      severity: "error"
```

### Performance Optimization Tips

1. **Partitioning Strategy**:
   ```yaml
   tables:
     large_table:
       partitionBy: ["year", "month", "day"]
       clusterBy: ["user_id"]
   ```

2. **Caching Intermediate Results**:
   ```java
   // Data sources automatically cache frequently accessed tables
   dataSource.cacheTable("intermediate_result", dataset);
   ```

3. **Adaptive Query Execution**:
   ```hocon
   spark.configs {
     "spark.sql.adaptive.enabled" = "true"
     "spark.sql.adaptive.coalescePartitions.enabled" = "true"
     "spark.sql.adaptive.skewJoin.enabled" = "true"
   }
   ```

## 🔧 Building and Testing

### Build Commands

```bash
# Clean build
mvn clean compile

# Run all tests
mvn test

# Package with dependencies
mvn package

# Skip tests (for faster builds)
mvn package -DskipTests

# Generate test coverage report
mvn jacoco:report
```

### Running Tests

```bash
# Unit tests only
mvn test -Dtest="*UnitTest"

# Integration tests
mvn test -Dtest="*IntegrationTest"

# Specific test class
mvn test -Dtest="DataContractLoaderTest"
```

### Local Development

```bash
# Run with local Spark
export SPARK_HOME=/path/to/spark
java -cp "target/classes:target/test-classes:$SPARK_HOME/jars/*" \
  io.sparkdataflow.core.SparkDataFlowApplication \
  src/test/resources/test-contract.yaml
```

## 🤝 Contributing

We welcome contributions! Please follow these guidelines:

### Development Workflow

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/SparkDataFlow.git
   cd SparkDataFlow
   ```

2. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Follow Code Standards**
   - Use consistent naming conventions
   - Add comprehensive JavaDoc comments
   - Follow existing code structure and patterns
   - Ensure thread safety where applicable

4. **Write Tests**
   - Unit tests for all new classes
   - Integration tests for end-to-end functionality
   - Maintain >80% code coverage

5. **Update Documentation**
   - Update README for new features
   - Add examples for new functionality
   - Update JSON schema for contract changes

### Code Review Checklist

- [ ] All tests pass
- [ ] Code coverage maintained
- [ ] Documentation updated
- [ ] Performance impact considered
- [ ] Security implications reviewed
- [ ] Backward compatibility maintained

### Submitting Changes

```bash
git add .
git commit -m "feat: add new data source adapter"
git push origin feature/your-feature-name
# Create pull request on GitHub
```

## 📚 Additional Resources

- [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [JSON Schema Specification](https://json-schema.org/)
- [Project Wiki](https://github.com/your-org/SparkDataFlow/wiki)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.