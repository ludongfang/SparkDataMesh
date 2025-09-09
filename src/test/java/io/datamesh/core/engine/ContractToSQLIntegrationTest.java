package io.datamesh.core.engine;

import io.sparkdataflow.core.contract.DataContract;
import io.sparkdataflow.core.contract.DataContractLoader;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

public class ContractToSQLIntegrationTest {

    private DataContractLoader contractLoader;
    private SQLGenerator sqlGenerator;
    private DataContract sampleContract;

    @Before
    public void setUp() throws Exception {
        contractLoader = new DataContractLoader();
        sqlGenerator = new SQLGenerator();
        
        // Load the sample transaction contract
        sampleContract = contractLoader.loadFromFile("src/main/resources/contracts/sample-transaction-contract.yaml");
    }

    @Test
    public void testLoadSampleContract() {
        Assert.assertNotNull("Contract should be loaded", sampleContract);
        Assert.assertEquals("Contract ID should match", "transaction-data-contract", sampleContract.getId());
        Assert.assertNotNull("Schema should be present", sampleContract.getSchema());
        Assert.assertNotNull("Tables should be present", sampleContract.getSchema().getTables());
        Assert.assertTrue("Should have source and target tables", sampleContract.getSchema().getTables().size() >= 2);
    }

    @Test
    public void testGenerateCreateTableSQLForSourceTable() {
        DataContract.Table rawTransactionsTable = sampleContract.getSchema().getTables().get("raw_transactions");
        Assert.assertNotNull("Raw transactions table should exist", rawTransactionsTable);

        String createTableSQL = sqlGenerator.generateCreateTableSQL("raw_transactions", rawTransactionsTable);
        
        Assert.assertNotNull("CREATE TABLE SQL should be generated", createTableSQL);
        Assert.assertTrue("Should contain CREATE TABLE statement", 
            createTableSQL.contains("CREATE TABLE IF NOT EXISTS raw_transactions"));
        Assert.assertTrue("Should contain transaction_id column", 
            createTableSQL.contains("transaction_id STRING NOT NULL"));
        Assert.assertTrue("Should contain amount column with precision", 
            createTableSQL.contains("amount DECIMAL(10,2) NOT NULL"));
        Assert.assertTrue("Should contain partitioning", 
            createTableSQL.contains("PARTITIONED BY (transaction_date)"));
        Assert.assertTrue("Should contain clustering", 
            createTableSQL.contains("CLUSTERED BY (status)"));
        Assert.assertTrue("Should specify storage format", 
            createTableSQL.contains("STORED AS PARQUET"));
        
        System.out.println("Generated CREATE TABLE SQL for raw_transactions:");
        System.out.println(createTableSQL);
        System.out.println();
    }

    @Test
    public void testGenerateCreateTableSQLForTargetTable() {
        DataContract.Table processedTransactionsTable = sampleContract.getSchema().getTables().get("processed_transactions");
        Assert.assertNotNull("Processed transactions table should exist", processedTransactionsTable);

        String createTableSQL = sqlGenerator.generateCreateTableSQL("processed_transactions", processedTransactionsTable);
        
        Assert.assertNotNull("CREATE TABLE SQL should be generated", createTableSQL);
        Assert.assertTrue("Should contain CREATE TABLE statement", 
            createTableSQL.contains("CREATE TABLE IF NOT EXISTS processed_transactions"));
        Assert.assertTrue("Should contain amount_usd column", 
            createTableSQL.contains("amount_usd DECIMAL(10,2) NOT NULL"));
        Assert.assertTrue("Should contain is_high_value boolean column", 
            createTableSQL.contains("is_high_value BOOLEAN NOT NULL"));
        Assert.assertTrue("Should contain risk_score double column", 
            createTableSQL.contains("risk_score DOUBLE"));
        Assert.assertTrue("Should contain partitioning", 
            createTableSQL.contains("PARTITIONED BY (transaction_date)"));
        
        System.out.println("Generated CREATE TABLE SQL for processed_transactions:");
        System.out.println(createTableSQL);
        System.out.println();
    }

    @Test
    public void testGenerateTransformationSQL() {
        DataContract.Transformation currencyConversion = sampleContract.getTransformations().get("currency_conversion");
        Assert.assertNotNull("Currency conversion transformation should exist", currencyConversion);

        // Create table mapping for the transformation
        Map<String, String> tableMapping = new HashMap<>();
        tableMapping.put("raw_transactions", "transaction_db.raw_transactions");
        tableMapping.put("processed_transactions", "transaction_db.processed_transactions");

        String transformationSQL = sqlGenerator.generateTransformationSQL(currencyConversion, tableMapping);
        
        Assert.assertNotNull("Transformation SQL should be generated", transformationSQL);
        Assert.assertTrue("Should contain SELECT statement", transformationSQL.contains("SELECT"));
        Assert.assertTrue("Should contain FROM clause with source table", 
            transformationSQL.contains("FROM transaction_db.raw_transactions"));
        
        // Check for specific field transformations
        Assert.assertTrue("Should contain transaction_id mapping", 
            transformationSQL.contains("transaction_id AS transaction_id"));
        Assert.assertTrue("Should contain amount_usd currency conversion logic", 
            transformationSQL.contains("CASE WHEN currency = 'USD' THEN amount ELSE amount * 1.0 END AS amount_usd"));
        Assert.assertTrue("Should contain is_high_value logic", 
            transformationSQL.contains("CASE WHEN amount >= 1000 THEN true ELSE false END AS is_high_value"));
        Assert.assertTrue("Should contain risk_score calculation", 
            transformationSQL.contains("CASE WHEN amount >= 1000 THEN 0.8 WHEN amount >= 500 THEN 0.5 ELSE 0.2 END AS risk_score"));
        
        System.out.println("Generated Transformation SQL for currency_conversion:");
        System.out.println(transformationSQL);
        System.out.println();
    }

    @Test
    public void testTransformationSQLFieldMapping() {
        DataContract.Transformation currencyConversion = sampleContract.getTransformations().get("currency_conversion");
        Map<String, String> tableMapping = new HashMap<>();
        tableMapping.put("raw_transactions", "transaction_db.raw_transactions");

        String transformationSQL = sqlGenerator.generateTransformationSQL(currencyConversion, tableMapping);
        
        // Verify all expected fields are present in the transformation
        String[] expectedFields = {
            "transaction_id", "user_id", "amount_usd", "original_amount", 
            "original_currency", "transaction_date", "status", "merchant_id", 
            "is_high_value", "risk_score"
        };
        
        for (String field : expectedFields) {
            Assert.assertTrue("Should contain field: " + field, 
                transformationSQL.contains(field));
        }
    }

    @Test
    public void testCompleteContractToSQLWorkflow() {
        // Test the complete workflow from contract to SQL generation
        Assert.assertNotNull("Contract should be loaded", sampleContract);
        
        // 1. Generate CREATE TABLE statements for all tables
        Map<String, String> createTableStatements = new HashMap<>();
        for (Map.Entry<String, DataContract.Table> tableEntry : sampleContract.getSchema().getTables().entrySet()) {
            String tableName = tableEntry.getKey();
            DataContract.Table table = tableEntry.getValue();
            String createSQL = sqlGenerator.generateCreateTableSQL(tableName, table);
            createTableStatements.put(tableName, createSQL);
            
            Assert.assertNotNull("CREATE TABLE SQL should be generated for " + tableName, createSQL);
            Assert.assertTrue("CREATE TABLE should contain table name: " + tableName, 
                createSQL.contains(tableName));
        }
        
        // 2. Generate transformation SQL statements
        Map<String, String> transformationStatements = new HashMap<>();
        Map<String, String> tableMapping = new HashMap<>();
        tableMapping.put("raw_transactions", "transaction_db.raw_transactions");
        tableMapping.put("processed_transactions", "transaction_db.processed_transactions");
        
        for (Map.Entry<String, DataContract.Transformation> transformEntry : sampleContract.getTransformations().entrySet()) {
            String transformName = transformEntry.getKey();
            DataContract.Transformation transform = transformEntry.getValue();
            String transformSQL = sqlGenerator.generateTransformationSQL(transform, tableMapping);
            transformationStatements.put(transformName, transformSQL);
            
            Assert.assertNotNull("Transformation SQL should be generated for " + transformName, transformSQL);
            Assert.assertTrue("Transformation SQL should contain SELECT: " + transformName, 
                transformSQL.contains("SELECT"));
        }
        
        // 3. Print the complete SQL workflow
        System.out.println("=== COMPLETE CONTRACT-TO-SQL WORKFLOW ===");
        System.out.println();
        
        System.out.println("1. CREATE TABLE STATEMENTS:");
        System.out.println("---------------------------");
        for (Map.Entry<String, String> entry : createTableStatements.entrySet()) {
            System.out.println("-- Table: " + entry.getKey());
            System.out.println(entry.getValue());
            System.out.println();
        }
        
        System.out.println("2. TRANSFORMATION STATEMENTS:");
        System.out.println("------------------------------");
        for (Map.Entry<String, String> entry : transformationStatements.entrySet()) {
            System.out.println("-- Transformation: " + entry.getKey());
            System.out.println(entry.getValue());
            System.out.println();
        }
        
        // 4. Validate that we have both source and target tables
        Assert.assertTrue("Should have raw_transactions table", 
            createTableStatements.containsKey("raw_transactions"));
        Assert.assertTrue("Should have processed_transactions table", 
            createTableStatements.containsKey("processed_transactions"));
        Assert.assertTrue("Should have currency_conversion transformation", 
            transformationStatements.containsKey("currency_conversion"));
    }

    @Test
    public void testSQLGenerationPerformance() {
        // Performance test for SQL generation
        long startTime = System.currentTimeMillis();
        
        Map<String, String> tableMapping = new HashMap<>();
        tableMapping.put("raw_transactions", "transaction_db.raw_transactions");
        
        // Generate SQL 100 times to test performance
        for (int i = 0; i < 100; i++) {
            for (DataContract.Table table : sampleContract.getSchema().getTables().values()) {
                sqlGenerator.generateCreateTableSQL("test_table_" + i, table);
            }
            
            for (DataContract.Transformation transform : sampleContract.getTransformations().values()) {
                sqlGenerator.generateTransformationSQL(transform, tableMapping);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        System.out.println("SQL Generation Performance Test:");
        System.out.println("Generated 200 SQL statements in " + duration + "ms");
        System.out.println("Average time per SQL statement: " + (duration / 200.0) + "ms");
        
        // Assert reasonable performance (should be fast)
        Assert.assertTrue("SQL generation should be fast (< 5 seconds for 200 statements)", 
            duration < 5000);
    }
}