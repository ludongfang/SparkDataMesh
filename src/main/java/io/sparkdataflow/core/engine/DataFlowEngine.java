package io.sparkdataflow.core.engine;

import io.sparkdataflow.core.contract.DataContract;
import io.sparkdataflow.core.domain.DomainDataSource;
import io.sparkdataflow.core.domain.DomainDataSourceFactory;
import io.sparkdataflow.core.validation.DataValidator;
import io.sparkdataflow.core.sink.DataSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DataFlowEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(DataFlowEngine.class);
    
    private final SparkSession spark;
    private final DomainDataSourceFactory dataSourceFactory;
    private final SQLGenerator sqlGenerator;
    private final DataValidator dataValidator;
    private final Map<String, Dataset<Row>> dataFrameRegistry;
    
    public DataFlowEngine(SparkSession spark) {
        this.spark = spark;
        this.dataSourceFactory = new DomainDataSourceFactory(spark);
        this.sqlGenerator = new SQLGenerator();
        this.dataValidator = new DataValidator();
        this.dataFrameRegistry = new HashMap<>();
    }
    
    public void executeDataContract(DataContract contract) {
        logger.info("Executing data contract: {}", contract.getId());
        
        try {
            loadSourceTables(contract);
            
            executeTransformations(contract);
            
            validateData(contract);
            
            sinkTargetTables(contract);
            
            logger.info("Successfully executed data contract: {}", contract.getId());
            
        } catch (Exception e) {
            logger.error("Failed to execute data contract: {}", contract.getId(), e);
            throw new RuntimeException("Data contract execution failed", e);
        } finally {
            cleanup();
        }
    }
    
    private void loadSourceTables(DataContract contract) {
        logger.info("Loading source tables");
        
        Map<String, DataContract.Table> tables = contract.getSchema().getTables();
        
        for (Map.Entry<String, DataContract.Table> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            DataContract.Table tableConfig = entry.getValue();
            
            if ("source".equals(tableConfig.getType())) {
                loadSourceTable(tableName, tableConfig, contract);
            }
        }
    }
    
    private void loadSourceTable(String tableName, DataContract.Table tableConfig, DataContract contract) {
        logger.info("Loading source table: {}", tableName);
        
        DataContract.Server serverConfig = findServerForDomain(tableConfig.getDomain(), contract);
        if (serverConfig == null) {
            throw new RuntimeException("No server configuration found for domain: " + tableConfig.getDomain());
        }
        
        DomainDataSource dataSource = dataSourceFactory.createDataSource(tableConfig.getDomain(), serverConfig);
        Dataset<Row> dataset = dataSource.readTable(tableName, tableConfig);
        
        dataFrameRegistry.put(tableName, dataset);
        dataset.createOrReplaceTempView(tableName);
        
        logger.info("Loaded and registered source table: {} with {} rows", tableName, dataset.count());
    }
    
    private void executeTransformations(DataContract contract) {
        logger.info("Executing transformations");
        
        if (contract.getTransformations() == null || contract.getTransformations().isEmpty()) {
            logger.info("No transformations defined");
            return;
        }
        
        Map<String, String> tableMapping = createTableMapping(contract);
        
        for (Map.Entry<String, DataContract.Transformation> entry : contract.getTransformations().entrySet()) {
            String transformationName = entry.getKey();
            DataContract.Transformation transformation = entry.getValue();
            
            executeTransformation(transformationName, transformation, tableMapping);
        }
    }
    
    private void executeTransformation(String name, DataContract.Transformation transformation, 
                                     Map<String, String> tableMapping) {
        logger.info("Executing transformation: {} of type: {}", name, transformation.getType());
        
        try {
            String sql = sqlGenerator.generateTransformationSQL(transformation, tableMapping);
            logger.debug("Generated SQL for transformation {}: {}", name, sql);
            
            Dataset<Row> result = spark.sql(sql);
            
            String targetTable = transformation.getTarget();
            dataFrameRegistry.put(targetTable, result);
            result.createOrReplaceTempView(targetTable);
            
            logger.info("Transformation {} completed successfully. Target table: {} has {} rows", 
                       name, targetTable, result.count());
            
        } catch (Exception e) {
            logger.error("Failed to execute transformation: {}", name, e);
            throw new RuntimeException("Transformation execution failed: " + name, e);
        }
    }
    
    private void validateData(DataContract contract) {
        logger.info("Validating data quality");
        
        if (contract.getQuality() == null || contract.getQuality().getExpectations() == null) {
            logger.info("No data quality expectations defined");
            return;
        }
        
        for (DataContract.Expectation expectation : contract.getQuality().getExpectations()) {
            validateExpectation(expectation);
        }
    }
    
    private void validateExpectation(DataContract.Expectation expectation) {
        logger.info("Validating expectation: {} on table: {}", expectation.getType(), expectation.getTable());
        
        Dataset<Row> dataset = dataFrameRegistry.get(expectation.getTable());
        if (dataset == null) {
            throw new RuntimeException("Table not found for validation: " + expectation.getTable());
        }
        
        boolean isValid = dataValidator.validateExpectation(dataset, expectation);
        
        if (!isValid) {
            String message = String.format("Data quality validation failed for expectation: %s on table: %s", 
                                         expectation.getType(), expectation.getTable());
            
            if ("error".equals(expectation.getSeverity())) {
                throw new RuntimeException(message);
            } else {
                logger.warn(message);
            }
        } else {
            logger.info("Data quality validation passed for expectation: {} on table: {}", 
                       expectation.getType(), expectation.getTable());
        }
    }
    
    private void sinkTargetTables(DataContract contract) {
        logger.info("Sinking target tables");
        
        Map<String, DataContract.Table> tables = contract.getSchema().getTables();
        
        for (Map.Entry<String, DataContract.Table> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            DataContract.Table tableConfig = entry.getValue();
            
            if ("target".equals(tableConfig.getType())) {
                sinkTargetTable(tableName, tableConfig, contract);
            }
        }
    }
    
    private void sinkTargetTable(String tableName, DataContract.Table tableConfig, DataContract contract) {
        logger.info("Sinking target table: {}", tableName);
        
        Dataset<Row> dataset = dataFrameRegistry.get(tableName);
        if (dataset == null) {
            throw new RuntimeException("Target table not found in registry: " + tableName);
        }
        
        DataContract.Server serverConfig = findServerForDomain(tableConfig.getDomain(), contract);
        if (serverConfig == null) {
            throw new RuntimeException("No server configuration found for domain: " + tableConfig.getDomain());
        }
        
        DataSink dataSink = createDataSink(serverConfig);
        dataSink.sinkTable(dataset, tableName, tableConfig);
        
        logger.info("Successfully sunk target table: {}", tableName);
    }
    
    private DataContract.Server findServerForDomain(String domain, DataContract contract) {
        if (contract.getServers() == null) {
            return null;
        }
        
        return contract.getServers().stream()
            .filter(server -> domain.equals(extractDomainFromServer(server)))
            .findFirst()
            .orElse(contract.getServers().get(0));
    }
    
    private String extractDomainFromServer(DataContract.Server server) {
        if (server.getDescription() != null && server.getDescription().contains("domain:")) {
            return server.getDescription().split("domain:")[1].trim();
        }
        return "default";
    }
    
    private Map<String, String> createTableMapping(DataContract contract) {
        Map<String, String> mapping = new HashMap<>();
        
        for (String tableName : contract.getSchema().getTables().keySet()) {
            mapping.put(tableName, tableName);
        }
        
        return mapping;
    }
    
    private DataSink createDataSink(DataContract.Server serverConfig) {
        return new DataSink(spark, serverConfig);
    }
    
    private void cleanup() {
        logger.info("Cleaning up resources");
        dataFrameRegistry.clear();
        dataSourceFactory.clearCache();
    }
    
    public Dataset<Row> getRegisteredTable(String tableName) {
        return dataFrameRegistry.get(tableName);
    }
    
    public Map<String, Dataset<Row>> getAllRegisteredTables() {
        return new HashMap<>(dataFrameRegistry);
    }
}