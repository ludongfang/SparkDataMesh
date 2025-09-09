package io.datamesh.core.domain;

import io.sparkdataflow.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class SparkTableDataSource extends DomainDataSource {
    
    private final String database;
    
    public SparkTableDataSource(SparkSession spark, String domain, String database) {
        super(spark, domain);
        this.database = database;
    }
    
    @Override
    public Dataset<Row> readTable(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        logger.info("Reading Spark table: {} for domain: {}", fullTableName, domain);
        
        try {
            Dataset<Row> dataset = spark.table(fullTableName);
            
            if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
                logger.debug("Table {} is partitioned by: {}", fullTableName, tableConfig.getPartitionBy());
            }
            
            return dataset;
        } catch (Exception e) {
            logger.error("Failed to read table: {} for domain: {}", fullTableName, domain, e);
            throw new RuntimeException("Failed to read table: " + fullTableName, e);
        }
    }
    
    @Override
    public StructType getTableSchema(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        try {
            return spark.table(fullTableName).schema();
        } catch (Exception e) {
            logger.error("Failed to get schema for table: {} in domain: {}", fullTableName, domain, e);
            throw new RuntimeException("Failed to get schema for table: " + fullTableName, e);
        }
    }
    
    @Override
    public boolean tableExists(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String fullTableName = database != null ? database + "." + physicalName : physicalName;
        
        try {
            spark.catalog().tableExists(fullTableName);
            return true;
        } catch (Exception e) {
            logger.debug("Table does not exist: {} in domain: {}", fullTableName, domain);
            return false;
        }
    }
    
    public String getDatabase() {
        return database;
    }
}