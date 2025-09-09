package io.datamesh.core.domain;

import io.sparkdataflow.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class ParquetDataSource extends DomainDataSource {
    
    private final String basePath;
    
    public ParquetDataSource(SparkSession spark, String domain, String basePath) {
        super(spark, domain);
        this.basePath = basePath;
    }
    
    @Override
    public Dataset<Row> readTable(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String tablePath = getTablePath(physicalName);
        
        logger.info("Reading Parquet table: {} from path: {} for domain: {}", physicalName, tablePath, domain);
        
        try {
            return spark.read().parquet(tablePath);
        } catch (Exception e) {
            logger.error("Failed to read Parquet table: {} from path: {} for domain: {}", physicalName, tablePath, domain, e);
            throw new RuntimeException("Failed to read Parquet table: " + physicalName, e);
        }
    }
    
    @Override
    public StructType getTableSchema(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String tablePath = getTablePath(physicalName);
        
        try {
            return spark.read().parquet(tablePath).schema();
        } catch (Exception e) {
            logger.error("Failed to get schema for Parquet table: {} from path: {} in domain: {}", physicalName, tablePath, domain, e);
            throw new RuntimeException("Failed to get schema for Parquet table: " + physicalName, e);
        }
    }
    
    @Override
    public boolean tableExists(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String tablePath = getTablePath(physicalName);
        
        try {
            spark.read().parquet(tablePath).schema();
            return true;
        } catch (Exception e) {
            logger.debug("Parquet table does not exist: {} at path: {} in domain: {}", physicalName, tablePath, domain);
            return false;
        }
    }
    
    private String getTablePath(String tableName) {
        if (basePath.endsWith("/")) {
            return basePath + tableName;
        } else {
            return basePath + "/" + tableName;
        }
    }
    
    public String getBasePath() {
        return basePath;
    }
}