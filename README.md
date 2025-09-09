# SparkDataMesh

A contract-driven data processing framework for Apache Spark that automates SQL generation and provides comprehensive data quality validation using the Open Data Contract Standard.

## Features

- **Contract-First Development**: Define transformations using YAML contracts
- **Multi-Source Support**: Spark tables, Delta Lake, Parquet, JSON, CSV
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
- **Data Sources**: Spark tables, Delta Lake, Parquet, JSON, CSV
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

## Example

See `src/main/resources/contracts/sample-transaction-contract.yaml` for a complete working example.

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