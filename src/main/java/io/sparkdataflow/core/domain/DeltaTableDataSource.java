package io.sparkdataflow.core.domain;

import io.sparkdataflow.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class DeltaTableDataSource extends DomainDataSource {
    
    private final String basePath;
    
    public DeltaTableDataSource(SparkSession spark, String domain, String basePath) {
        super(spark, domain);
        this.basePath = basePath;
    }
    
    @Override
    public Dataset<Row> readTable(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String tablePath = getTablePath(physicalName);
        
        logger.info("Reading Delta table: {} from path: {} for domain: {}", physicalName, tablePath, domain);
        
        try {
            return spark.read().format("delta").load(tablePath);
        } catch (Exception e) {
            logger.error("Failed to read Delta table: {} from path: {} for domain: {}", physicalName, tablePath, domain, e);
            throw new RuntimeException("Failed to read Delta table: " + physicalName, e);
        }
    }
    
    @Override
    public StructType getTableSchema(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String tablePath = getTablePath(physicalName);
        
        try {
            return spark.read().format("delta").load(tablePath).schema();
        } catch (Exception e) {
            logger.error("Failed to get schema for Delta table: {} from path: {} in domain: {}", physicalName, tablePath, domain, e);
            throw new RuntimeException("Failed to get schema for Delta table: " + physicalName, e);
        }
    }
    
    @Override
    public boolean tableExists(String tableName, DataContract.Table tableConfig) {
        String physicalName = getPhysicalTableName(tableName, tableConfig);
        String tablePath = getTablePath(physicalName);
        
        try {
            spark.read().format("delta").load(tablePath).schema();
            return true;
        } catch (Exception e) {
            logger.debug("Delta table does not exist: {} at path: {} in domain: {}", physicalName, tablePath, domain);
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