# SparkDataMesh

A contract-driven data processing framework for Apache Spark that automates SQL generation and provides comprehensive data quality validation using the Open Data Contract Standard.

## Features

- **Contract-First Development**: Define transformations using YAML contracts
- **Multi-Source Support**: Spark tables, Hive tables, Delta Lake, Parquet, JSON, CSV
- **SQL Generation**: Auto-generates optimized Spark SQL from rules
- **Data Quality**: Built-in validators with configurable severity levels
- **Domain-Driven**: Organize processing by business domains

## Quick Start

### Prerequisites
- Java 17+
- Apache Maven 3.6+

### 1. Build
```bash
mvn clean package
```

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

### 3. Run
```bash
java -cp target/spark-data-flow-1.0-SNAPSHOT.jar \
  io.sparkdataflow.core.SparkDataFlowApplication \
  contract.yaml
```

## Architecture

**Data Contract** → **SQL Generation** → **Validation** → **Execution**

### Components
- **Data Sources**: Spark tables, Hive tables, Delta Lake, Parquet, JSON, CSV
- **Transformations**: extract, transform, aggregate, join, filter, union
- **Validators**: not_null, unique, range, pattern, freshness, completeness, custom

## Contract Structure

```yaml
dataContractSpecification: "1.0.0"
id: "pipeline-name"
info:
  title: "Pipeline Title"
  version: "1.0.0"

servers:
  - type: "spark"
    environment: "production"
    connectionString: "database:db"

schema:
  tables:
    source_table:
      type: "source"
      domain: "transaction"
      fields:
        field_name:
          type: "string"
          required: true

transformations:
  my_transformation:
    type: "transform"
    source: "source_table"
    target: "target_table"
    rules:
      - field: "new_field"
        operation: "map"
        expression: "UPPER(old_field)"

quality:
  expectations:
    - type: "not_null"
      table: "source_table"
      field: "required_field"
      severity: "error"
```

## Hive Integration

SparkDataMesh supports Hive tables through Spark's built-in Hive integration, enabling seamless processing of data stored in Hive.

### Hive Data Contract Example

```yaml
dataContractSpecification: "1.0.0"
id: "hive-sales-processing"
info:
  title: "Sales Data Processing with Hive"
  version: "1.0.0"

servers:
  - type: "hive"
    environment: "production"
    description: "Production Hive cluster"
    connectionString: "hive://sales_db?metastore=thrift://hive-metastore:9083"

schema:
  tables:
    raw_sales:
      type: "source"
      domain: "sales"
      physicalName: "raw_sales"
      fields:
        sale_id:
          type: "string"
          required: true
          primaryKey: true
        total_amount:
          type: "decimal"
          precision: 10
          scale: 2
        sale_date:
          type: "date"
        region:
          type: "string"
          constraints:
            enum: ["north", "south", "east", "west"]
      partitionBy: ["sale_date", "region"]
      clusterBy: ["sale_id"]

transformations:
  process_sales:
    type: "transform"
    source: "raw_sales"
    target: "processed_sales"
    rules:
      - field: "revenue_category"
        operation: "map"
        expression: "CASE WHEN total_amount >= 1000 THEN 'high' ELSE 'low' END"
```

### Hive Connection Strings

Hive connection strings support the following formats:

- Basic database: `hive://database_name`
- With metastore URI: `hive://database_name?metastore=thrift://host:port`
- Legacy format: `database:database_name` (uses default metastore settings)

### Hive Configuration

Configure Hive settings in `application.conf`:

```hocon
hive {
  metastoreUris = "thrift://localhost:9083"
  defaultDatabase = "default"
  warehouseDir = "/tmp/hive-warehouse"
  
  auth {
    enabled = false
    principal = ""
    keytab = ""
  }
  
  connection {
    timeoutMs = 60000
    retryIntervalMs = 5000
    maxRetries = 3
  }
}

spark {
  configs {
    "spark.sql.catalogImplementation" = "hive"
    "javax.jdo.option.ConnectionURL" = "jdbc:derby:memory:metastore_db;create=true"
    "javax.jdo.option.ConnectionDriverName" = "org.apache.derby.jdbc.EmbeddedDriver"
  }
}
```

### Hive Features

- **Partitioning**: Automatic partition discovery and pruning
- **Bucketing**: Support for clustered tables
- **Metastore Integration**: Connect to external Hive metastore
- **Schema Evolution**: Handle schema changes gracefully
- **Data Types**: Full support for Hive data types
- **DDL Operations**: Create tables from contract definitions

## Configuration

```hocon
spark {
  appName = "SparkDataMesh"
  master = "local[*]"
  configs {
    "spark.sql.adaptive.enabled" = "true"
    "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
  }
}
validation.enabled = true
```

## Examples

- **Transaction Processing**: `src/main/resources/contracts/sample-transaction-contract.yaml`
- **Hive Integration**: `src/main/resources/contracts/sample-hive-contract.yaml`

## Project Structure

```
src/main/java/io/sparkdataflow/core/
├── contract/     # Data contract models and loader
├── domain/       # Domain data source implementations  
├── engine/       # SQL generation and execution
├── validation/   # Data validation framework
└── sink/         # Data sink implementations
```

## Development

```bash
# Build
mvn clean package

# Test
mvn test

# Run
java -cp target/spark-data-mesh-1.0-SNAPSHOT.jar \
  io.sparkdataflow.core.SparkDataFlowApplication \
  contract.yaml
```

## Contributing

1. Fork and create feature branch
2. Follow existing code patterns
3. Add tests for new functionality
4. Update documentation
5. Submit pull request

## Resources

- [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

## License

MIT License - see [LICENSE](LICENSE) file.