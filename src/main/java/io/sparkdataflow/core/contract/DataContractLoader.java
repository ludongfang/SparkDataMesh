package io.sparkdataflow.core.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DataContractLoader {
    
    private static final Logger logger = LoggerFactory.getLogger(DataContractLoader.class);
    private final ObjectMapper jsonMapper;
    private final ObjectMapper yamlMapper;
    
    public DataContractLoader() {
        this.jsonMapper = new ObjectMapper();
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
        logger.info("DataContractLoader initialized with YAML-first approach");
    }
    
    /**
     * Validates the basic structure and required fields of a data contract
     * @param contract The data contract to validate
     * @return List of validation errors (empty if valid)
     */
    private List<String> validateContractStructure(DataContract contract) {
        List<String> errors = new ArrayList<>();
        
        // Check required top-level fields
        if (contract.getDataContractSpecification() == null || contract.getDataContractSpecification().isEmpty()) {
            errors.add("dataContractSpecification is required");
        } else if (!"1.0.0".equals(contract.getDataContractSpecification())) {
            errors.add("dataContractSpecification must be '1.0.0'");
        }
        
        if (contract.getId() == null || contract.getId().isEmpty()) {
            errors.add("id is required");
        }
        
        if (contract.getInfo() == null) {
            errors.add("info section is required");
        } else {
            if (contract.getInfo().getTitle() == null || contract.getInfo().getTitle().isEmpty()) {
                errors.add("info.title is required");
            }
            if (contract.getInfo().getVersion() == null || contract.getInfo().getVersion().isEmpty()) {
                errors.add("info.version is required");
            }
        }
        
        if (contract.getSchema() == null) {
            errors.add("schema section is required");
        } else if (contract.getSchema().getTables() == null || contract.getSchema().getTables().isEmpty()) {
            errors.add("schema.tables is required and must not be empty");
        } else {
            // Validate tables
            contract.getSchema().getTables().forEach((tableName, table) -> {
                if (table.getType() == null || table.getType().isEmpty()) {
                    errors.add("Table '" + tableName + "' must have a type (source, target, or intermediate)");
                }
                if (table.getFields() == null || table.getFields().isEmpty()) {
                    errors.add("Table '" + tableName + "' must have fields defined");
                } else {
                    // Validate fields
                    table.getFields().forEach((fieldName, field) -> {
                        if (field.getType() == null || field.getType().isEmpty()) {
                            errors.add("Field '" + fieldName + "' in table '" + tableName + "' must have a type");
                        }
                    });
                }
            });
        }
        
        // Validate transformations if present
        if (contract.getTransformations() != null) {
            contract.getTransformations().forEach((transformName, transformation) -> {
                if (transformation.getType() == null || transformation.getType().isEmpty()) {
                    errors.add("Transformation '" + transformName + "' must have a type");
                }
                if (transformation.getSource() == null) {
                    errors.add("Transformation '" + transformName + "' must have a source");
                }
                if (transformation.getTarget() == null || transformation.getTarget().isEmpty()) {
                    errors.add("Transformation '" + transformName + "' must have a target");
                }
            });
        }
        
        return errors;
    }
    
    public DataContract loadFromFile(String filePath) throws IOException, IllegalArgumentException {
        logger.info("Loading data contract from file: {}", filePath);
        
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        DataContract contract;
        boolean isYaml = filePath.endsWith(".yaml") || filePath.endsWith(".yml");
        
        if (isYaml) {
            contract = yamlMapper.readValue(content, DataContract.class);
            logger.debug("Parsed YAML contract: {}", contract.getId());
        } else if (filePath.endsWith(".json")) {
            contract = jsonMapper.readValue(content, DataContract.class);
            logger.debug("Parsed JSON contract: {}", contract.getId());
        } else {
            throw new IllegalArgumentException("Unsupported file format. Only JSON and YAML are supported.");
        }
        
        List<String> validationErrors = validateContractStructure(contract);
        if (!validationErrors.isEmpty()) {
            String errorMessage = "Data contract validation failed:\\n" + String.join("\\n", validationErrors);
            logger.error("Data contract validation failed: {}", errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        
        logger.info("Successfully loaded and validated data contract: {}", contract.getId());
        return contract;
    }
    
    public DataContract loadFromString(String content, boolean isYaml) throws IOException, IllegalArgumentException {
        logger.info("Loading data contract from string content");
        
        DataContract contract;
        if (isYaml) {
            contract = yamlMapper.readValue(content, DataContract.class);
        } else {
            contract = jsonMapper.readValue(content, DataContract.class);
        }
        
        List<String> validationErrors = validateContractStructure(contract);
        if (!validationErrors.isEmpty()) {
            String errorMessage = "Data contract validation failed:\\n" + String.join("\\n", validationErrors);
            logger.error("Data contract validation failed: {}", errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
        
        logger.info("Successfully loaded and validated data contract: {}", contract.getId());
        return contract;
    }
    
    public void saveToFile(DataContract contract, String filePath) throws IOException {
        logger.info("Saving data contract to file: {}", filePath);
        
        boolean isYaml = filePath.endsWith(".yaml") || filePath.endsWith(".yml");
        
        if (isYaml) {
            yamlMapper.writeValue(Paths.get(filePath).toFile(), contract);
            logger.debug("Saved contract as YAML: {}", filePath);
        } else if (filePath.endsWith(".json")) {
            jsonMapper.writerWithDefaultPrettyPrinter().writeValue(Paths.get(filePath).toFile(), contract);
            logger.debug("Saved contract as JSON: {}", filePath);
        } else {
            throw new IllegalArgumentException("Unsupported file format. Only JSON and YAML are supported.");
        }
        
        logger.info("Successfully saved data contract: {}", contract.getId());
    }
    
    /**
     * Validates a YAML contract file directly without loading the full contract
     * @param yamlFilePath Path to the YAML contract file
     * @return true if valid, false otherwise
     */
    public boolean validateYamlContract(String yamlFilePath) {
        try {
            DataContract contract = loadContractFromYaml(yamlFilePath);
            List<String> errors = validateContractStructure(contract);
            if (errors.isEmpty()) {
                logger.info("YAML contract validation successful: {}", yamlFilePath);
                return true;
            } else {
                logger.error("YAML contract validation failed for {}: {}", yamlFilePath, String.join(", ", errors));
                return false;
            }
        } catch (Exception e) {
            logger.error("YAML contract validation failed for {}: {}", yamlFilePath, e.getMessage());
            return false;
        }
    }
    
    /**
     * Loads a DataContract from a YAML file without validation
     * @param yamlFilePath Path to the YAML contract file
     * @return DataContract object
     */
    private DataContract loadContractFromYaml(String yamlFilePath) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(yamlFilePath)));
        return yamlMapper.readValue(content, DataContract.class);
    }
    
}