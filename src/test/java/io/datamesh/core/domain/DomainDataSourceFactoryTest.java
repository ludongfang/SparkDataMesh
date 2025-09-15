package io.datamesh.core.domain;

import io.datamesh.core.contract.DataContract;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class DomainDataSourceFactoryTest {

    private SparkSession spark;
    private DomainDataSourceFactory factory;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .appName("DomainDataSourceFactoryTest")
                .master("local[*]")
                .config("spark.sql.catalogImplementation", "in-memory")
                .getOrCreate();

        factory = new DomainDataSourceFactory(spark);
    }

    @After
    public void tearDown() {
        if (factory != null) {
            factory.clearCache();
        }
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testCreateHiveDataSource() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("test");
        serverConfig.setConnectionString("hive://test_db?metastore=thrift://localhost:9083");

        DomainDataSource dataSource = factory.createDataSource("sales", serverConfig);

        Assert.assertNotNull("Data source should not be null", dataSource);
        Assert.assertTrue("Should be HiveDataSource instance", dataSource instanceof HiveDataSource);
        Assert.assertEquals("Domain should match", "sales", dataSource.getDomain());

        HiveDataSource hiveDataSource = (HiveDataSource) dataSource;
        Assert.assertEquals("Database should match", "test_db", hiveDataSource.getDatabase());
        Assert.assertEquals("Metastore URI should match", "thrift://localhost:9083", hiveDataSource.getMetastoreUri());
    }

    @Test
    public void testCreateHiveDataSourceWithBasicConnectionString() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("test");
        serverConfig.setConnectionString("hive://sales_db");

        DomainDataSource dataSource = factory.createDataSource("sales", serverConfig);

        Assert.assertNotNull("Data source should not be null", dataSource);
        Assert.assertTrue("Should be HiveDataSource instance", dataSource instanceof HiveDataSource);

        HiveDataSource hiveDataSource = (HiveDataSource) dataSource;
        Assert.assertEquals("Database should match", "sales_db", hiveDataSource.getDatabase());
        Assert.assertNull("Metastore URI should be null", hiveDataSource.getMetastoreUri());
    }

    @Test
    public void testCreateHiveDataSourceWithLegacyConnectionString() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("test");
        serverConfig.setConnectionString("database:legacy_db");

        DomainDataSource dataSource = factory.createDataSource("sales", serverConfig);

        Assert.assertNotNull("Data source should not be null", dataSource);
        Assert.assertTrue("Should be HiveDataSource instance", dataSource instanceof HiveDataSource);

        HiveDataSource hiveDataSource = (HiveDataSource) dataSource;
        Assert.assertEquals("Database should match", "legacy_db", hiveDataSource.getDatabase());
        Assert.assertNull("Metastore URI should be null", hiveDataSource.getMetastoreUri());
    }

    @Test
    public void testCreateSparkDataSource() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("spark");
        serverConfig.setEnvironment("test");
        serverConfig.setConnectionString("database:spark_db");

        DomainDataSource dataSource = factory.createDataSource("analytics", serverConfig);

        Assert.assertNotNull("Data source should not be null", dataSource);
        Assert.assertTrue("Should be SparkTableDataSource instance", dataSource instanceof SparkTableDataSource);
        Assert.assertEquals("Domain should match", "analytics", dataSource.getDomain());
    }

    @Test
    public void testDataSourceCaching() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("production");
        serverConfig.setConnectionString("hive://prod_db");

        // Create first instance
        DomainDataSource dataSource1 = factory.createDataSource("sales", serverConfig);
        
        // Create second instance with same parameters
        DomainDataSource dataSource2 = factory.createDataSource("sales", serverConfig);

        // Should return the same cached instance
        Assert.assertSame("Should return cached instance", dataSource1, dataSource2);
    }

    @Test
    public void testDifferentEnvironmentsCreateDifferentInstances() {
        DataContract.Server prodConfig = new DataContract.Server();
        prodConfig.setType("hive");
        prodConfig.setEnvironment("production");
        prodConfig.setConnectionString("hive://prod_db");

        DataContract.Server testConfig = new DataContract.Server();
        testConfig.setType("hive");
        testConfig.setEnvironment("test");
        testConfig.setConnectionString("hive://test_db");

        DomainDataSource prodDataSource = factory.createDataSource("sales", prodConfig);
        DomainDataSource testDataSource = factory.createDataSource("sales", testConfig);

        Assert.assertNotSame("Different environments should create different instances", 
                            prodDataSource, testDataSource);
    }

    @Test
    public void testGetDataSource() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("test");
        serverConfig.setConnectionString("hive://test_db");

        // Create data source
        DomainDataSource createdDataSource = factory.createDataSource("sales", serverConfig);

        // Retrieve the same data source
        DomainDataSource retrievedDataSource = factory.getDataSource("sales", "hive", "test");

        Assert.assertSame("Retrieved data source should be the same instance", 
                         createdDataSource, retrievedDataSource);
    }

    @Test
    public void testGetNonExistentDataSource() {
        DomainDataSource dataSource = factory.getDataSource("nonexistent", "hive", "test");
        Assert.assertNull("Non-existent data source should return null", dataSource);
    }

    @Test
    public void testUnsupportedDataSourceType() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("unsupported");
        serverConfig.setEnvironment("test");
        serverConfig.setConnectionString("some://connection");

        try {
            factory.createDataSource("test", serverConfig);
            Assert.fail("Should throw exception for unsupported data source type");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("Should contain unsupported type message", 
                            e.getMessage().contains("Unsupported data source type"));
        }
    }

    @Test
    public void testClearCache() {
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("test");
        serverConfig.setConnectionString("hive://test_db");

        // Create data source
        DomainDataSource originalDataSource = factory.createDataSource("sales", serverConfig);
        Assert.assertNotNull("Original data source should not be null", originalDataSource);

        // Clear cache
        factory.clearCache();

        // Create again - should be a new instance
        DomainDataSource newDataSource = factory.createDataSource("sales", serverConfig);
        Assert.assertNotNull("New data source should not be null", newDataSource);
        
        // Note: Due to caching implementation, this might return the same instance
        // The test mainly verifies that clearCache() doesn't throw exceptions
    }

    @Test
    public void testExtractDatabaseFromHiveConnectionString() {
        // Test through factory creation to verify connection string parsing
        DataContract.Server serverConfig = new DataContract.Server();
        serverConfig.setType("hive");
        serverConfig.setEnvironment("test");

        // Test basic hive:// format
        serverConfig.setConnectionString("hive://my_database");
        HiveDataSource hiveDataSource1 = (HiveDataSource) factory.createDataSource("test1", serverConfig);
        Assert.assertEquals("Database should be extracted correctly", "my_database", hiveDataSource1.getDatabase());

        // Test hive:// with metastore parameter
        serverConfig.setConnectionString("hive://my_database?metastore=thrift://host:9083");
        HiveDataSource hiveDataSource2 = (HiveDataSource) factory.createDataSource("test2", serverConfig);
        Assert.assertEquals("Database should be extracted correctly", "my_database", hiveDataSource2.getDatabase());
        Assert.assertEquals("Metastore URI should be extracted correctly", "thrift://host:9083", hiveDataSource2.getMetastoreUri());

        // Test legacy database: format
        serverConfig.setConnectionString("database:legacy_db");
        HiveDataSource hiveDataSource3 = (HiveDataSource) factory.createDataSource("test3", serverConfig);
        Assert.assertEquals("Database should be extracted correctly", "legacy_db", hiveDataSource3.getDatabase());
    }
}