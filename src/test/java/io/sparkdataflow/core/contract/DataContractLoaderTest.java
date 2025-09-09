package io.sparkdataflow.core.contract;

import org.junit.Test;
import org.junit.Assert;

public class DataContractLoaderTest {

    @Test
    public void testValidYamlContractValidation() {
        DataContractLoader loader = new DataContractLoader();
        boolean isValid = loader.validateYamlContract("src/test/resources/test-contract-valid.yaml");
        Assert.assertTrue("Valid YAML contract should pass validation", isValid);
    }

    @Test
    public void testInvalidYamlContractValidation() {
        DataContractLoader loader = new DataContractLoader();
        boolean isValid = loader.validateYamlContract("src/test/resources/test-contract-invalid.yaml");
        Assert.assertFalse("Invalid YAML contract should fail validation", isValid);
    }

    @Test
    public void testLoadValidYamlContract() {
        DataContractLoader loader = new DataContractLoader();
        try {
            DataContract contract = loader.loadFromFile("src/test/resources/test-contract-valid.yaml");
            
            Assert.assertNotNull("Contract should not be null", contract);
            Assert.assertEquals("Contract ID should match", "test-contract", contract.getId());
            Assert.assertEquals("Contract title should match", "Test Data Contract", contract.getInfo().getTitle());
            Assert.assertEquals("Contract version should match", "1.0.0", contract.getInfo().getVersion());
            
        } catch (Exception e) {
            Assert.fail("Should not throw exception for valid contract: " + e.getMessage());
        }
    }

    @Test
    public void testLoadInvalidYamlContract() {
        DataContractLoader loader = new DataContractLoader();
        try {
            loader.loadFromFile("src/test/resources/test-contract-invalid.yaml");
            Assert.fail("Should throw exception for invalid contract");
        } catch (IllegalArgumentException e) {
            // Expected exception for validation failure
            Assert.assertTrue("Should contain validation error message", 
                e.getMessage().contains("validation failed"));
        } catch (Exception e) {
            Assert.fail("Unexpected exception type: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }
    }
}