package io.datamesh.core.domain;

import io.datamesh.core.contract.DataContract;
import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for HiveDataSource that don't require Spark initialization
 */
public class HiveDataSourceUnitTest {

    @Test
    public void testConnectionStringParsing() {
        // Test factory connection string parsing through the factory
        DomainDataSourceFactory factory = new DomainDataSourceFactory(null); // No Spark session needed for this test
        
        // We can't actually create the data source without Spark, but we can test the parsing logic
        // by examining the factory's private methods through reflection
        try {
            java.lang.reflect.Method extractDbMethod = DomainDataSourceFactory.class
                    .getDeclaredMethod("extractDatabaseFromConnectionString", String.class);
            extractDbMethod.setAccessible(true);
            
            java.lang.reflect.Method extractMetastoreMethod = DomainDataSourceFactory.class
                    .getDeclaredMethod("extractMetastoreUriFromConnectionString", String.class);
            extractMetastoreMethod.setAccessible(true);
            
            // Test basic hive:// format
            String result = (String) extractDbMethod.invoke(factory, "hive://my_database");
            Assert.assertEquals("Database should be extracted correctly", "my_database", result);
            
            String metastore = (String) extractMetastoreMethod.invoke(factory, "hive://my_database");
            Assert.assertNull("Metastore should be null for basic format", metastore);
            
            // Test hive:// with metastore parameter
            result = (String) extractDbMethod.invoke(factory, "hive://my_database?metastore=thrift://host:9083");
            Assert.assertEquals("Database should be extracted correctly", "my_database", result);
            
            metastore = (String) extractMetastoreMethod.invoke(factory, "hive://my_database?metastore=thrift://host:9083");
            Assert.assertEquals("Metastore URI should be extracted correctly", "thrift://host:9083", metastore);
            
            // Test legacy database: format
            result = (String) extractDbMethod.invoke(factory, "database:legacy_db");
            Assert.assertEquals("Database should be extracted correctly", "legacy_db", result);
            
            metastore = (String) extractMetastoreMethod.invoke(factory, "database:legacy_db");
            Assert.assertNull("Metastore should be null for legacy format", metastore);
            
            // Test null/empty inputs
            result = (String) extractDbMethod.invoke(factory, (String) null);
            Assert.assertNull("Null input should return null", result);
            
            result = (String) extractDbMethod.invoke(factory, "");
            Assert.assertNull("Empty input should return null", result);
            
        } catch (Exception e) {
            Assert.fail("Failed to test connection string parsing: " + e.getMessage());
        }
    }

    @Test
    public void testServerConfigurationTypes() {
        // Test that server configuration properly identifies Hive type
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("production");
        serverConfig.setConnectionString("hive://sales_db?metastore=thrift://localhost:9083");
        
        Assert.assertEquals("Type should be hive", "hive", serverConfig.getType());
        Assert.assertEquals("Environment should be production", "production", serverConfig.getEnvironment());
        Assert.assertEquals("Connection string should match", "hive://sales_db?metastore=thrift://localhost:9083", serverConfig.getConnectionString());
    }

    @Test
    public void testTableConfigurationWithHiveFeatures() {
        // Test table configuration for Hive-specific features
        DataContract.Table table = new DataContract.Table();
        table.setType("source");
        table.setDomain("sales");
        table.setPhysicalName("raw_sales");
        
        // Test partitioning
        table.setPartitionBy(Arrays.asList("sale_date", "region"));
        Assert.assertEquals("Should have 2 partition columns", 2, table.getPartitionBy().size());
        Assert.assertEquals("First partition should be sale_date", "sale_date", table.getPartitionBy().get(0));
        Assert.assertEquals("Second partition should be region", "region", table.getPartitionBy().get(1));
        
        // Test clustering (bucketing in Hive)
        table.setClusterBy(Arrays.asList("customer_id"));
        Assert.assertEquals("Should have 1 cluster column", 1, table.getClusterBy().size());
        Assert.assertEquals("Cluster column should be customer_id", "customer_id", table.getClusterBy().get(0));
        
        // Test fields with Hive-compatible types
        Map<String, DataContract.Field> fields = new HashMap<>();
        
        DataContract.Field stringField = new DataContract.Field();
        stringField.setType("string");
        stringField.setRequired(true);
        fields.put("id", stringField);
        
        DataContract.Field decimalField = new DataContract.Field();
        decimalField.setType("decimal");
        decimalField.setPrecision(10);
        decimalField.setScale(2);
        fields.put("amount", decimalField);
        
        DataContract.Field timestampField = new DataContract.Field();
        timestampField.setType("timestamp");
        fields.put("created_at", timestampField);
        
        table.setFields(fields);
        
        Assert.assertEquals("Should have 3 fields", 3, table.getFields().size());
        Assert.assertEquals("String field type should match", "string", table.getFields().get("id").getType());
        Assert.assertEquals("Decimal field precision should match", Integer.valueOf(10), table.getFields().get("amount").getPrecision());
        Assert.assertEquals("Decimal field scale should match", Integer.valueOf(2), table.getFields().get("amount").getScale());
        Assert.assertEquals("Timestamp field type should match", "timestamp", table.getFields().get("created_at").getType());
    }

    @Test
    public void testDataContractStructureForHive() {
        // Test complete data contract structure for Hive
        DataContract contract = new DataContract();
        contract.setId("hive-test-contract");
        
        DataContract.ContractInfo info = new DataContract.ContractInfo();
        info.setTitle("Hive Test Contract");
        info.setVersion("1.0.0");
        contract.setInfo(info);
        
        // Test server configuration
        DataContract.Server server = new DataContract.Server();
        server.setType("hive");
        server.setEnvironment("test");
        server.setConnectionString("hive://test_db");
        contract.setServers(Arrays.asList(server));
        
        // Test schema
        DataContract.Schema schema = new DataContract.Schema();
        Map<String, DataContract.Table> tables = new HashMap<>();
        
        DataContract.Table table = new DataContract.Table();
        table.setType("source");
        table.setDomain("test");
        tables.put("test_table", table);
        
        schema.setTables(tables);
        contract.setSchema(schema);
        
        // Verify contract structure
        Assert.assertEquals("Contract ID should match", "hive-test-contract", contract.getId());
        Assert.assertEquals("Contract title should match", "Hive Test Contract", contract.getInfo().getTitle());
        Assert.assertEquals("Server type should be hive", "hive", contract.getServers().get(0).getType());
        Assert.assertEquals("Table domain should match", "test", contract.getSchema().getTables().get("test_table").getDomain());
    }

    @Test
    public void testHiveConnectionStringVariations() {
        // Test various connection string formats that Hive data source should support
        String[] validConnections = {
            "hive://database_name",
            "hive://database_name?metastore=thrift://localhost:9083",
            "hive://my_db?metastore=thrift://hive-metastore:9083&timeout=30000",
            "database:legacy_database",
            "database:another_db"
        };
        
        for (String connection : validConnections) {
            DataContract.Server serverConfig = new DataContract.Server();
            serverConfig.setType("hive");
            serverConfig.setEnvironment("test");
            serverConfig.setConnectionString(connection);
            
            Assert.assertNotNull("Connection string should not be null", serverConfig.getConnectionString());
            Assert.assertFalse("Connection string should not be empty", serverConfig.getConnectionString().isEmpty());
            Assert.assertEquals("Connection string should match input", connection, serverConfig.getConnectionString());
        }
    }

    @Test
    public void testHiveQualityExpectations() {
        // Test that quality expectations work with Hive tables
        DataContract.Quality quality = new DataContract.Quality();
        
        DataContract.Expectation notNullExpectation = new DataContract.Expectation();
        notNullExpectation.setType("not_null");
        notNullExpectation.setTable("hive_table");
        notNullExpectation.setField("id");
        notNullExpectation.setSeverity("error");
        
        DataContract.Expectation rangeExpectation = new DataContract.Expectation();
        rangeExpectation.setType("range");
        rangeExpectation.setTable("hive_table");
        rangeExpectation.setField("amount");
        rangeExpectation.setExpression("0,1000000");
        rangeExpectation.setSeverity("warning");
        
        DataContract.Expectation freshnessExpectation = new DataContract.Expectation();
        freshnessExpectation.setType("freshness");
        freshnessExpectation.setTable("hive_table");
        freshnessExpectation.setField("created_date");
        freshnessExpectation.setExpression("24");
        freshnessExpectation.setSeverity("warning");
        
        quality.setExpectations(Arrays.asList(notNullExpectation, rangeExpectation, freshnessExpectation));
        
        Assert.assertEquals("Should have 3 expectations", 3, quality.getExpectations().size());
        Assert.assertEquals("First expectation should be not_null", "not_null", quality.getExpectations().get(0).getType());
        Assert.assertEquals("Second expectation should be range", "range", quality.getExpectations().get(1).getType());
        Assert.assertEquals("Third expectation should be freshness", "freshness", quality.getExpectations().get(2).getType());
        
        // Verify Hive table references
        for (DataContract.Expectation expectation : quality.getExpectations()) {
            Assert.assertEquals("All expectations should reference hive_table", "hive_table", expectation.getTable());
        }
    }
}