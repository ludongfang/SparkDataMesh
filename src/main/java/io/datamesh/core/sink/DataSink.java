package io.datamesh.core.sink;

import io.datamesh.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSink {
    
    private static final Logger logger = LoggerFactory.getLogger(DataSink.class);
    
    private final SparkSession spark;
    private final DataContract.Server serverConfig;
    
    public DataSink(SparkSession spark, DataContract.Server serverConfig) {
        this.spark = spark;
        this.serverConfig = serverConfig;
    }
    
    public void sinkTable(Dataset<Row> dataset, String tableName, DataContract.Table tableConfig) {
        logger.info("Sinking table: {} using server type: {}", tableName, serverConfig.getType());
        
        try {
            switch (serverConfig.getType().toLowerCase()) {
                case "spark":
                case "hive":
                    sinkToSparkTable(dataset, tableName, tableConfig);
                    break;
                case "delta":
                    sinkToDeltaTable(dataset, tableName, tableConfig);
                    break;
                case "parquet":
                    sinkToParquet(dataset, tableName, tableConfig);
                    break;
                case "json":
                    sinkToJson(dataset, tableName, tableConfig);
                    break;
                case "csv":
                    sinkToCsv(dataset, tableName, tableConfig);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported sink type: " + serverConfig.getType());
            }
            
            logger.info("Successfully sunk table: {} with {} rows", tableName, dataset.count());
            
        } catch (Exception e) {
            logger.error("Failed to sink table: {}", tableName, e);
            throw new RuntimeException("Failed to sink table: " + tableName, e);
        }
    }
    
    private void sinkToSparkTable(Dataset<Row> dataset, String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = getFullTableName(physicalName);
        
        logger.info("Sinking to Spark table: {}", fullTableName);
        
        if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .partitionBy(tableConfig.getPartitionBy().toArray(new String[0]))
                .saveAsTable(fullTableName);
        } else {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable(fullTableName);
        }
    }
    
    private void sinkToDeltaTable(Dataset<Row> dataset, String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String path = getPath(physicalName);
        
        logger.info("Sinking to Delta table at path: {}", path);
        
        if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
            dataset.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .partitionBy(tableConfig.getPartitionBy().toArray(new String[0]))
                .option("path", path)
                .saveAsTable(physicalName);
        } else {
            dataset.write()
                .format("delta")
                .mode(SaveMode.Overwrite)
                .option("path", path)
                .saveAsTable(physicalName);
        }
    }
    
    private void sinkToParquet(Dataset<Row> dataset, String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String path = getPath(physicalName);
        
        logger.info("Sinking to Parquet at path: {}", path);
        
        if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .partitionBy(tableConfig.getPartitionBy().toArray(new String[0]))
                .parquet(path);
        } else {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .parquet(path);
        }
    }
    
    private void sinkToJson(Dataset<Row> dataset, String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String path = getPath(physicalName);
        
        logger.info("Sinking to JSON at path: {}", path);
        
        if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .partitionBy(tableConfig.getPartitionBy().toArray(new String[0]))
                .json(path);
        } else {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .json(path);
        }
    }
    
    private void sinkToCsv(Dataset<Row> dataset, String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String path = getPath(physicalName);
        
        logger.info("Sinking to CSV at path: {}", path);
        
        if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .partitionBy(tableConfig.getPartitionBy().toArray(new String[0]))
                .csv(path);
        } else {
            dataset.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(path);
        }
    }
    
    private String getPhysicalTableName(String logicalName, DataContract.Table tableConfig) {
        return tableConfig.getPhysicalName() != null ? 
               tableConfig.getPhysicalName() : 
               logicalName;
    }
    
    private String getFullTableName(String tableName) {
        String connectionString = serverConfig.getConnectionString();
        
        if (connectionString != null && connectionString.startsWith("database:")) {
            String database = connectionString.substring("database:".length());
            return database + "." + tableName;
        }
        
        return tableName;
    }
    
    private String getPath(String tableName) {
        String connectionString = serverConfig.getConnectionString();
        
        if (connectionString != null && !connectionString.startsWith("database:")) {
            if (connectionString.endsWith("/")) {
                return connectionString + tableName;
            } else {
                return connectionString + "/" + tableName;
            }
        }
        
        return "/tmp/sparkdataflow/" + tableName;
    }
}