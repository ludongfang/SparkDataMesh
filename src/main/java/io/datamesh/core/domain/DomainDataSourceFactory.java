package io.datamesh.core.domain;

import io.datamesh.core.contract.DataContract;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DomainDataSourceFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(DomainDataSourceFactory.class);
    private final SparkSession spark;
    private final Map<String, DomainDataSource> dataSources;
    
    public DomainDataSourceFactory(SparkSession spark) {
        this.spark = spark;
        this.dataSources = new HashMap<>();
    }
    
    public DomainDataSource createDataSource(String domain, DataContract.Server serverConfig) {
        String key = domain + "_" + serverConfig.getType() + "_" + serverConfig.getEnvironment();
        
        return dataSources.computeIfAbsent(key, k -> {
            logger.info("Creating data source for domain: {} with type: {} in environment: {}", 
                       domain, serverConfig.getType(), serverConfig.getEnvironment());
            
            switch (serverConfig.getType().toLowerCase()) {
                case "spark":
                    return createSparkTableDataSource(domain, serverConfig);
                case "hive":
                    return createHiveDataSource(domain, serverConfig);
                case "delta":
                    return createDeltaTableDataSource(domain, serverConfig);
                case "parquet":
                    return createParquetDataSource(domain, serverConfig);
                case "json":
                    return createJsonDataSource(domain, serverConfig);
                case "csv":
                    return createCsvDataSource(domain, serverConfig);
                default:
                    throw new IllegalArgumentException("Unsupported data source type: " + serverConfig.getType());
            }
        });
    }
    
    private DomainDataSource createSparkTableDataSource(String domain, DataContract.Server serverConfig) {
        String database = extractDatabaseFromConnectionString(serverConfig.getConnectionString());
        return new SparkTableDataSource(spark, domain, database);
    }
    
    private DomainDataSource createHiveDataSource(String domain, DataContract.Server serverConfig) {
        String database = extractDatabaseFromConnectionString(serverConfig.getConnectionString());
        String metastoreUri = extractMetastoreUriFromConnectionString(serverConfig.getConnectionString());
        return new HiveDataSource(spark, domain, database, metastoreUri);
    }
    
    private DomainDataSource createDeltaTableDataSource(String domain, DataContract.Server serverConfig) {
        return new DeltaTableDataSource(spark, domain, serverConfig.getConnectionString());
    }
    
    private DomainDataSource createParquetDataSource(String domain, DataContract.Server serverConfig) {
        return new ParquetDataSource(spark, domain, serverConfig.getConnectionString());
    }
    
    private DomainDataSource createJsonDataSource(String domain, DataContract.Server serverConfig) {
        return new JsonDataSource(spark, domain, serverConfig.getConnectionString());
    }
    
    private DomainDataSource createCsvDataSource(String domain, DataContract.Server serverConfig) {
        return new CsvDataSource(spark, domain, serverConfig.getConnectionString());
    }
    
    private String extractDatabaseFromConnectionString(String connectionString) {
        if (connectionString == null || connectionString.isEmpty()) {
            return null;
        }
        
        if (connectionString.startsWith("database:")) {
            return connectionString.substring("database:".length());
        }
        
        // Handle Hive connection string format: hive://database?metastore=uri
        if (connectionString.startsWith("hive://")) {
            String withoutProtocol = connectionString.substring("hive://".length());
            if (withoutProtocol.contains("?")) {
                return withoutProtocol.split("\\?")[0];
            }
            return withoutProtocol;
        }
        
        return connectionString;
    }
    
    private String extractMetastoreUriFromConnectionString(String connectionString) {
        if (connectionString == null || connectionString.isEmpty()) {
            return null;
        }
        
        // Handle Hive connection string format: hive://database?metastore=uri
        if (connectionString.startsWith("hive://") && connectionString.contains("metastore=")) {
            String[] parts = connectionString.split("\\?");
            if (parts.length > 1) {
                String[] params = parts[1].split("&");
                for (String param : params) {
                    if (param.startsWith("metastore=")) {
                        return param.substring("metastore=".length());
                    }
                }
            }
        }
        
        return null;
    }
    
    public DomainDataSource getDataSource(String domain, String type, String environment) {
        String key = domain + "_" + type + "_" + environment;
        return dataSources.get(key);
    }
    
    public void clearCache() {
        dataSources.values().forEach(DomainDataSource::uncacheAll);
        dataSources.clear();
        logger.info("Cleared all data source caches");
    }
}