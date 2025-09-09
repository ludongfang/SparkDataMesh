package io.sparkdataflow.core.contract;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class DataContract {
    
    @JsonProperty("dataContractSpecification")
    private String dataContractSpecification;
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("info")
    private ContractInfo info;
    
    @JsonProperty("servers")
    private List<Server> servers;
    
    @JsonProperty("schema")
    private Schema schema;
    
    @JsonProperty("transformations")
    private Map<String, Transformation> transformations;
    
    @JsonProperty("quality")
    private Quality quality;

    public static class ContractInfo {
        @JsonProperty("title")
        private String title;
        
        @JsonProperty("version")
        private String version;
        
        @JsonProperty("description")
        private String description;
        
        @JsonProperty("owner")
        private String owner;
        
        @JsonProperty("contact")
        private Contact contact;

        public String getTitle() { return title; }
        public void setTitle(String title) { this.title = title; }
        public String getVersion() { return version; }
        public void setVersion(String version) { this.version = version; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getOwner() { return owner; }
        public void setOwner(String owner) { this.owner = owner; }
        public Contact getContact() { return contact; }
        public void setContact(Contact contact) { this.contact = contact; }
    }

    public static class Contact {
        @JsonProperty("name")
        private String name;
        
        @JsonProperty("email")
        private String email;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
    }

    public static class Server {
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("environment")
        private String environment;
        
        @JsonProperty("description")
        private String description;
        
        @JsonProperty("connectionString")
        private String connectionString;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getEnvironment() { return environment; }
        public void setEnvironment(String environment) { this.environment = environment; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getConnectionString() { return connectionString; }
        public void setConnectionString(String connectionString) { this.connectionString = connectionString; }
    }

    public static class Schema {
        @JsonProperty("tables")
        private Map<String, Table> tables;

        public Map<String, Table> getTables() { return tables; }
        public void setTables(Map<String, Table> tables) { this.tables = tables; }
    }

    public static class Table {
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("description")
        private String description;
        
        @JsonProperty("domain")
        private String domain;
        
        @JsonProperty("physicalName")
        private String physicalName;
        
        @JsonProperty("fields")
        private Map<String, Field> fields;
        
        @JsonProperty("partitionBy")
        private List<String> partitionBy;
        
        @JsonProperty("clusterBy")
        private List<String> clusterBy;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getDomain() { return domain; }
        public void setDomain(String domain) { this.domain = domain; }
        public String getPhysicalName() { return physicalName; }
        public void setPhysicalName(String physicalName) { this.physicalName = physicalName; }
        public Map<String, Field> getFields() { return fields; }
        public void setFields(Map<String, Field> fields) { this.fields = fields; }
        public List<String> getPartitionBy() { return partitionBy; }
        public void setPartitionBy(List<String> partitionBy) { this.partitionBy = partitionBy; }
        public List<String> getClusterBy() { return clusterBy; }
        public void setClusterBy(List<String> clusterBy) { this.clusterBy = clusterBy; }
    }

    public static class Field {
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("description")
        private String description;
        
        @JsonProperty("required")
        private Boolean required = true;
        
        @JsonProperty("nullable")
        private Boolean nullable = true;
        
        @JsonProperty("primaryKey")
        private Boolean primaryKey = false;
        
        @JsonProperty("constraints")
        private Map<String, Object> constraints;
        
        @JsonProperty("format")
        private String format;
        
        @JsonProperty("precision")
        private Integer precision;
        
        @JsonProperty("scale")
        private Integer scale;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public Boolean getRequired() { return required; }
        public void setRequired(Boolean required) { this.required = required; }
        public Boolean getNullable() { return nullable; }
        public void setNullable(Boolean nullable) { this.nullable = nullable; }
        public Boolean getPrimaryKey() { return primaryKey; }
        public void setPrimaryKey(Boolean primaryKey) { this.primaryKey = primaryKey; }
        public Map<String, Object> getConstraints() { return constraints; }
        public void setConstraints(Map<String, Object> constraints) { this.constraints = constraints; }
        public String getFormat() { return format; }
        public void setFormat(String format) { this.format = format; }
        public Integer getPrecision() { return precision; }
        public void setPrecision(Integer precision) { this.precision = precision; }
        public Integer getScale() { return scale; }
        public void setScale(Integer scale) { this.scale = scale; }
    }

    public static class Transformation {
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("description")
        private String description;
        
        @JsonProperty("source")
        private Object source;
        
        @JsonProperty("target")
        private String target;
        
        @JsonProperty("sql")
        private String sql;
        
        @JsonProperty("rules")
        private List<TransformationRule> rules;

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public Object getSource() { return source; }
        public void setSource(Object source) { this.source = source; }
        public String getTarget() { return target; }
        public void setTarget(String target) { this.target = target; }
        public String getSql() { return sql; }
        public void setSql(String sql) { this.sql = sql; }
        public List<TransformationRule> getRules() { return rules; }
        public void setRules(List<TransformationRule> rules) { this.rules = rules; }
    }

    public static class TransformationRule {
        @JsonProperty("field")
        private String field;
        
        @JsonProperty("operation")
        private String operation;
        
        @JsonProperty("expression")
        private String expression;
        
        @JsonProperty("condition")
        private String condition;

        public String getField() { return field; }
        public void setField(String field) { this.field = field; }
        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
        public String getExpression() { return expression; }
        public void setExpression(String expression) { this.expression = expression; }
        public String getCondition() { return condition; }
        public void setCondition(String condition) { this.condition = condition; }
    }

    public static class Quality {
        @JsonProperty("expectations")
        private List<Expectation> expectations;

        public List<Expectation> getExpectations() { return expectations; }
        public void setExpectations(List<Expectation> expectations) { this.expectations = expectations; }
    }

    public static class Expectation {
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("table")
        private String table;
        
        @JsonProperty("field")
        private String field;
        
        @JsonProperty("description")
        private String description;
        
        @JsonProperty("expression")
        private String expression;
        
        @JsonProperty("threshold")
        private Double threshold;
        
        @JsonProperty("severity")
        private String severity = "error";

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }
        public String getField() { return field; }
        public void setField(String field) { this.field = field; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getExpression() { return expression; }
        public void setExpression(String expression) { this.expression = expression; }
        public Double getThreshold() { return threshold; }
        public void setThreshold(Double threshold) { this.threshold = threshold; }
        public String getSeverity() { return severity; }
        public void setSeverity(String severity) { this.severity = severity; }
    }

    public String getDataContractSpecification() { return dataContractSpecification; }
    public void setDataContractSpecification(String dataContractSpecification) { this.dataContractSpecification = dataContractSpecification; }
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public ContractInfo getInfo() { return info; }
    public void setInfo(ContractInfo info) { this.info = info; }
    public List<Server> getServers() { return servers; }
    public void setServers(List<Server> servers) { this.servers = servers; }
    public Schema getSchema() { return schema; }
    public void setSchema(Schema schema) { this.schema = schema; }
    public Map<String, Transformation> getTransformations() { return transformations; }
    public void setTransformations(Map<String, Transformation> transformations) { this.transformations = transformations; }
    public Quality getQuality() { return quality; }
    public void setQuality(Quality quality) { this.quality = quality; }
}