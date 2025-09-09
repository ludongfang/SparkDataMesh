package io.datamesh.core.domain;

import io.sparkdataflow.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class DomainDataSource {
    
    protected static final Logger logger = LoggerFactory.getLogger(DomainDataSource.class);
    protected final SparkSession spark;
    protected final String domain;
    protected final Map<String, Dataset<Row>> cachedTables;
    
    public DomainDataSource(SparkSession spark, String domain) {
        this.spark = spark;
        this.domain = domain;
        this.cachedTables = new HashMap<>();
    }
    
    public abstract Dataset<Row> readTable(String tableName, DataContract.Table tableConfig);
    
    public abstract StructType getTableSchema(String tableName, DataContract.Table tableConfig);
    
    public abstract boolean tableExists(String tableName, DataContract.Table tableConfig);
    
    public Dataset<Row> getCachedTable(String tableName) {
        return cachedTables.get(tableName);
    }
    
    public void cacheTable(String tableName, Dataset<Row> dataset) {
        dataset.cache();
        cachedTables.put(tableName, dataset);
        logger.info("Cached table: {} for domain: {}", tableName, domain);
    }
    
    public void uncacheTable(String tableName) {
        Dataset<Row> dataset = cachedTables.remove(tableName);
        if (dataset != null) {
            dataset.unpersist();
            logger.info("Uncached table: {} for domain: {}", tableName, domain);
        }
    }
    
    public void uncacheAll() {
        cachedTables.forEach((name, dataset) -> {
            dataset.unpersist();
            logger.info("Uncached table: {} for domain: {}", name, domain);
        });
        cachedTables.clear();
    }
    
    public String getDomain() {
        return domain;
    }
    
    protected String getPhysicalTableName(String logicalName, DataContract.Table tableConfig) {
        return tableConfig.getPhysicalName() != null ? 
               tableConfig.getPhysicalName() : 
               domain + "_" + logicalName;
    }
}