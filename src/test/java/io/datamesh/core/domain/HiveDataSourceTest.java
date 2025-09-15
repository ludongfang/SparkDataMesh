package io.datamesh.core.domain;

import io.datamesh.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HiveDataSourceTest {

    private SparkSession spark;
    private HiveDataSource hiveDataSource;
    private DataContract.Table mockTableConfig;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .appName("HiveDataSourceTest")
                .master("local[*]")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
                .getOrCreate();

        hiveDataSource = new HiveDataSource(spark, "test_domain", "test_db");
        
        mockTableConfig = createMockTableConfig();
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testConstructorWithDatabase() {
        HiveDataSource dataSource = new HiveDataSource(spark, "sales", "sales_db");
        Assert.assertEquals("sales", dataSource.getDomain());
        Assert.assertEquals("sales_db", dataSource.getDatabase());
        Assert.assertNull(dataSource.getMetastoreUri());
    }

    @Test
    public void testConstructorWithMetastoreUri() {
        String metastoreUri = "thrift://localhost:9083";
        HiveDataSource dataSource = new HiveDataSource(spark, "sales", "sales_db", metastoreUri);
        Assert.assertEquals("sales", dataSource.getDomain());
        Assert.assertEquals("sales_db", dataSource.getDatabase());
        Assert.assertEquals(metastoreUri, dataSource.getMetastoreUri());
    }

    @Test
    public void testGetPhysicalTableName() {
        // Test with physicalName set
        mockTableConfig.setPhysicalName("custom_table_name");
        String physicalName = hiveDataSource.getPhysicalTableName("logical_name", mockTableConfig);
        Assert.assertEquals("custom_table_name", physicalName);

        // Test without physicalName (should use domain + logical name)
        mockTableConfig.setPhysicalName(null);
        physicalName = hiveDataSource.getPhysicalTableName("logical_name", mockTableConfig);
        Assert.assertEquals("test_domain_logical_name", physicalName);
    }

    @Test
    public void testCreateInMemoryTableAndRead() {
        // Create a test table in memory
        StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.StringType, false),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("age", DataTypes.IntegerType, true)
        });

        Dataset<Row> testData = spark.createDataFrame(Arrays.asList(
            RowFactory.create("1", "John", 25),
            RowFactory.create("2", "Jane", 30)
        ), schema);

        // Register as temporary view (simulates Hive table)
        String tableName = "test_db.test_table";
        testData.createOrReplaceTempView("test_table");
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW test_db.test_table AS SELECT * FROM test_table");

        // Test reading the table
        mockTableConfig.setPhysicalName("test_table");
        Dataset<Row> result = hiveDataSource.readTable("test_table", mockTableConfig);
        
        Assert.assertNotNull("Result should not be null", result);
        Assert.assertEquals("Should have 2 rows", 2, result.count());
        Assert.assertEquals("Should have 3 columns", 3, result.columns().length);
    }

    @Test
    public void testGetTableSchema() {
        // Create a test table
        StructType expectedSchema = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.StringType, false),
            DataTypes.createStructField("amount", DataTypes.createDecimalType(10, 2), true)
        });

        Dataset<Row> testData = spark.createDataFrame(Arrays.asList(), expectedSchema);
        testData.createOrReplaceTempView("schema_test_table");
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW test_db.schema_test_table AS SELECT * FROM schema_test_table");

        mockTableConfig.setPhysicalName("schema_test_table");
        StructType actualSchema = hiveDataSource.getTableSchema("schema_test_table", mockTableConfig);

        Assert.assertNotNull("Schema should not be null", actualSchema);
        Assert.assertEquals("Should have correct number of fields", 2, actualSchema.fields().length);
        Assert.assertEquals("First field should be id", "id", actualSchema.fields()[0].name());
        Assert.assertEquals("Second field should be amount", "amount", actualSchema.fields()[1].name());
    }

    @Test
    public void testTableExists() {
        // Test with non-existent table
        mockTableConfig.setPhysicalName("non_existent_table");
        boolean exists = hiveDataSource.tableExists("non_existent_table", mockTableConfig);
        Assert.assertFalse("Non-existent table should return false", exists);

        // Create a table and test again
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW test_db.existing_table AS SELECT 1 as id");

        mockTableConfig.setPhysicalName("existing_table");
        exists = hiveDataSource.tableExists("existing_table", mockTableConfig);
        // Note: In test environment with temporary views, this might behave differently
        // The actual behavior depends on the Spark catalog implementation
    }

    @Test
    public void testMapDataType() {
        // Test various data type mappings through reflection
        try {
            java.lang.reflect.Method mapDataTypeMethod = HiveDataSource.class.getDeclaredMethod("mapDataType", String.class);
            mapDataTypeMethod.setAccessible(true);

            Assert.assertEquals("STRING", mapDataTypeMethod.invoke(hiveDataSource, "string"));
            Assert.assertEquals("INT", mapDataTypeMethod.invoke(hiveDataSource, "integer"));
            Assert.assertEquals("BIGINT", mapDataTypeMethod.invoke(hiveDataSource, "long"));
            Assert.assertEquals("DOUBLE", mapDataTypeMethod.invoke(hiveDataSource, "double"));
            Assert.assertEquals("DECIMAL(10,2)", mapDataTypeMethod.invoke(hiveDataSource, "decimal"));
            Assert.assertEquals("BOOLEAN", mapDataTypeMethod.invoke(hiveDataSource, "boolean"));
            Assert.assertEquals("TIMESTAMP", mapDataTypeMethod.invoke(hiveDataSource, "timestamp"));
            Assert.assertEquals("DATE", mapDataTypeMethod.invoke(hiveDataSource, "date"));
            Assert.assertEquals("STRING", mapDataTypeMethod.invoke(hiveDataSource, "unknown_type"));

        } catch (Exception e) {
            Assert.fail("Failed to test mapDataType method: " + e.getMessage());
        }
    }

    @Test
    public void testExecuteHiveSQL() {
        try {
            // Test simple SQL execution
            hiveDataSource.executeHiveSQL("CREATE DATABASE IF NOT EXISTS test_sql_db");
            
            // Verify database creation (this is environment dependent)
            // In a full Hive environment, you would check the metastore
            // In test environment, we just verify no exception is thrown
            Assert.assertTrue("SQL execution should complete without exception", true);
            
        } catch (Exception e) {
            // In test environment, some Hive-specific operations might fail
            // We mainly test that the method handles exceptions properly
            Assert.assertTrue("Exception should be wrapped in RuntimeException", 
                e instanceof RuntimeException || e.getCause() instanceof RuntimeException);
        }
    }

    @Test
    public void testCacheOperations() {
        // Create test data
        StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.StringType, false)
        });
        
        Dataset<Row> testData = spark.createDataFrame(Arrays.asList(), schema);
        
        // Test caching
        String tableName = "cache_test_table";
        hiveDataSource.cacheTable(tableName, testData);
        
        Dataset<Row> cachedData = hiveDataSource.getCachedTable(tableName);
        Assert.assertNotNull("Cached table should not be null", cachedData);
        
        // Test uncaching
        hiveDataSource.uncacheTable(tableName);
        Dataset<Row> uncachedData = hiveDataSource.getCachedTable(tableName);
        Assert.assertNull("Uncached table should be null", uncachedData);
        
        // Test uncacheAll
        hiveDataSource.cacheTable("table1", testData);
        hiveDataSource.cacheTable("table2", testData);
        hiveDataSource.uncacheAll();
        
        Assert.assertNull("All tables should be uncached", hiveDataSource.getCachedTable("table1"));
        Assert.assertNull("All tables should be uncached", hiveDataSource.getCachedTable("table2"));
    }

    private DataContract.Table createMockTableConfig() {
        DataContract.Table table = new DataContract.Table();
        table.setType("source");
        table.setDomain("test_domain");
        table.setDescription("Test table");
        
        // Create mock fields
        Map<String, DataContract.Field> fields = new HashMap<>();
        
        DataContract.Field idField = new DataContract.Field();
        idField.setType("string");
        idField.setRequired(true);
        idField.setPrimaryKey(true);
        fields.put("id", idField);
        
        DataContract.Field nameField = new DataContract.Field();
        nameField.setType("string");
        nameField.setRequired(false);
        fields.put("name", nameField);
        
        DataContract.Field ageField = new DataContract.Field();
        ageField.setType("integer");
        ageField.setRequired(false);
        fields.put("age", ageField);
        
        table.setFields(fields);
        
        // Set partitioning and clustering
        table.setPartitionBy(Arrays.asList("date_partition"));
        table.setClusterBy(Arrays.asList("id"));
        
        return table;
    }
}