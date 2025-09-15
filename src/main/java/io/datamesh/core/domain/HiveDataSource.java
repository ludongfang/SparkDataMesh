package io.datamesh.core.domain;

import io.datamesh.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Hive data source implementation for reading Hive tables through Spark
 * Supports Hive metastore integration and Hive-specific configurations
 */
public class HiveDataSource extends DomainDataSource {
    
    private final String database;
    private final String metastoreUri;
    
    public HiveDataSource(SparkSession spark, String domain, String database) {
        this(spark, domain, database, null);
    }
    
    public HiveDataSource(SparkSession spark, String domain, String database, String metastoreUri) {
        super(spark, domain);
        this.database = database;
        this.metastoreUri = metastoreUri;
        
        // Enable Hive support in Spark if not already enabled
        enableHiveSupport();
    }
    
    @Override
    public Dataset<Row> readTable(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        logger.info("Reading Hive table: {} for domain: {}", fullTableName, domain);
        
        try {
            // Use Spark's Hive integration to read the table
            Dataset<Row> dataset = spark.table(fullTableName);
            
            // Log partition information if available
            if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
                logger.debug("Hive table {} is partitioned by: {}", fullTableName, tableConfig.getPartitionBy());
            }
            
            // Log cluster information if available
            if (tableConfig.getClusterBy() != null && !tableConfig.getClusterBy().isEmpty()) {
                logger.debug("Hive table {} is clustered by: {}", fullTableName, tableConfig.getClusterBy());
            }
            
            return dataset;
        } catch (Exception e) {
            logger.error("Failed to read Hive table: {} for domain: {}", fullTableName, domain, e);
            throw new RuntimeException("Failed to read Hive table: " + fullTableName, e);
        }
    }
    
    @Override
    public StructType getTableSchema(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        try {
            return spark.table(fullTableName).schema();
        } catch (Exception e) {
            logger.error("Failed to get schema for Hive table: {} in domain: {}", fullTableName, domain, e);
            throw new RuntimeException("Failed to get schema for Hive table: " + fullTableName, e);
        }
    }
    
    @Override
    public boolean tableExists(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        try {
            return spark.catalog().tableExists(fullTableName);
        } catch (Exception e) {
            logger.debug("Hive table does not exist: {} in domain: {}", fullTableName, domain);
            return false;
        }
    }
    
    /**
     * Execute Hive-specific SQL commands for DDL operations
     */
    public void executeHiveSQL(String sql) {
        logger.info("Executing Hive SQL for domain {}: {}", domain, sql);
        try {
            spark.sql(sql);
        } catch (Exception e) {
            logger.error("Failed to execute Hive SQL for domain: {}", domain, e);
            throw new RuntimeException("Failed to execute Hive SQL", e);
        }
    }
    
    /**
     * Create a Hive table based on the contract definition
     */
    public void createTable(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        if (tableExists(tableName, tableConfig)) {
            logger.info("Hive table {} already exists, skipping creation", fullTableName);
            return;
        }
        
        StringBuilder createTableSQL = new StringBuilder();
        createTableSQL.append("CREATE TABLE ").append(fullTableName).append(" (");
        
        // Add field definitions (simplified - would need full implementation)
        tableConfig.getFields().forEach((fieldName, field) -> {
            createTableSQL.append(fieldName).append(" ").append(mapDataType(field.getType()));
            if (field.getRequired() != null && !field.getRequired()) {
                // Hive doesn't have explicit nullable syntax, handled by data types
            }
            createTableSQL.append(", ");
        });
        
        // Remove trailing comma
        if (createTableSQL.toString().endsWith(", ")) {
            createTableSQL.setLength(createTableSQL.length() - 2);
        }
        createTableSQL.append(")");
        
        // Add partitioning
        if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
            createTableSQL.append(" PARTITIONED BY (");
            tableConfig.getPartitionBy().forEach(partition -> {
                // Get partition field type from fields definition
                DataContract.Field partitionField = tableConfig.getFields().get(partition);
                if (partitionField != null) {
                    createTableSQL.append(partition).append(" ").append(mapDataType(partitionField.getType())).append(", ");
                }
            });
            if (createTableSQL.toString().endsWith(", ")) {
                createTableSQL.setLength(createTableSQL.length() - 2);
            }
            createTableSQL.append(")");
        }
        
        // Add clustering (bucketing in Hive)
        if (tableConfig.getClusterBy() != null && !tableConfig.getClusterBy().isEmpty()) {
            createTableSQL.append(" CLUSTERED BY (");
            tableConfig.getClusterBy().forEach(cluster -> createTableSQL.append(cluster).append(", "));
            if (createTableSQL.toString().endsWith(", ")) {
                createTableSQL.setLength(createTableSQL.length() - 2);
            }
            createTableSQL.append(") INTO 10 BUCKETS"); // Default bucket count
        }
        
        executeHiveSQL(createTableSQL.toString());
        logger.info("Created Hive table: {} for domain: {}", fullTableName, domain);
    }
    
    /**
     * Map contract data types to Hive data types
     */
    private String mapDataType(String contractType) {
        switch (contractType.toLowerCase()) {
            case "string":
                return "STRING";
            case "integer":
            case "int":
                return "INT";
            case "long":
                return "BIGINT";
            case "double":
            case "float":
                return "DOUBLE";
            case "decimal":
                return "DECIMAL(10,2)"; // Default precision
            case "boolean":
                return "BOOLEAN";
            case "timestamp":
                return "TIMESTAMP";
            case "date":
                return "DATE";
            case "binary":
                return "BINARY";
            default:
                logger.warn("Unknown data type: {}, defaulting to STRING", contractType);
                return "STRING";
        }
    }
    
    /**
     * Enable Hive support in Spark session if not already enabled
     */
    private void enableHiveSupport() {
        try {
            // Check if Hive support is already available
            spark.sql("SHOW DATABASES");
            logger.debug("Hive support is available for domain: {}", domain);
        } catch (Exception e) {
            logger.warn("Hive support may not be properly configured for domain: {}", domain, e);
        }
        
        // Configure metastore URI if provided
        if (metastoreUri != null && !metastoreUri.isEmpty()) {
            spark.conf().set("hive.metastore.uris", metastoreUri);
            logger.info("Configured Hive metastore URI: {} for domain: {}", metastoreUri, domain);
        }
    }
    
    /**
     * Get partition information for a Hive table
     */
    public Dataset<Row> getPartitions(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        try {
            return spark.sql("SHOW PARTITIONS " + fullTableName);
        } catch (Exception e) {
            logger.error("Failed to get partitions for Hive table: {} in domain: {}", fullTableName, domain, e);
            throw new RuntimeException("Failed to get partitions for Hive table: " + fullTableName, e);
        }
    }
    
    /**
     * Refresh table metadata from Hive metastore
     */
    public void refreshTable(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        try {
            spark.sql("REFRESH TABLE " + fullTableName);
            logger.info("Refreshed Hive table metadata: {} for domain: {}", fullTableName, domain);
        } catch (Exception e) {
            logger.error("Failed to refresh Hive table: {} for domain: {}", fullTableName, domain, e);
            throw new RuntimeException("Failed to refresh Hive table: " + fullTableName, e);
        }
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getMetastoreUri() {
        return metastoreUri;
    }
}