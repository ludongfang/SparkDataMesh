package io.datamesh.core;

import io.datamesh.core.config.ApplicationConfig;
import io.datamesh.core.contract.DataContract;
import io.datamesh.core.contract.DataContractLoader;
import io.datamesh.core.engine.DataFlowEngine;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkDataFlowApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(SparkDataFlowApplication.class);
    
    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Usage: SparkDataFlowApplication <data-contract-file-path> [config-file-path]");
            System.exit(1);
        }
        
        String contractFilePath = args[0];
        String configFilePath = args.length > 1 ? args[1] : null;
        
        try {
            ApplicationConfig config = loadConfiguration(configFilePath);
            
            SparkSession spark = initializeSparkSession(config);
            
            DataContractLoader contractLoader = new DataContractLoader();
            DataContract contract = contractLoader.loadFromFile(contractFilePath);
            
            DataFlowEngine engine = new DataFlowEngine(spark);
            
            logger.info("Starting data flow execution for contract: {}", contract.getId());
            long startTime = System.currentTimeMillis();
            
            engine.executeDataContract(contract);
            
            long endTime = System.currentTimeMillis();
            logger.info("Data flow execution completed successfully in {} ms", (endTime - startTime));
            
        } catch (Exception e) {
            logger.error("Data flow execution failed", e);
            System.exit(1);
        }
    }
    
    private static ApplicationConfig loadConfiguration(String configFilePath) {
        try {
            if (configFilePath != null) {
                return ApplicationConfig.loadFromFile(configFilePath);
            } else {
                return ApplicationConfig.loadDefault();
            }
        } catch (Exception e) {
            logger.warn("Failed to load configuration, using defaults", e);
            return ApplicationConfig.createDefault();
        }
    }
    
    private static SparkSession initializeSparkSession(ApplicationConfig config) {
        logger.info("Initializing Spark session with app name: {}", config.getSparkAppName());
        
        SparkSession.Builder builder = SparkSession.builder()
            .appName(config.getSparkAppName());
        
        if (config.getSparkMaster() != null) {
            builder.master(config.getSparkMaster());
        }
        
        if (config.getSparkConfigs() != null) {
            config.getSparkConfigs().forEach(builder::config);
        }
        
        SparkSession spark = builder.getOrCreate();
        
        logger.info("Spark session initialized with version: {}", spark.version());
        return spark;
    }
}