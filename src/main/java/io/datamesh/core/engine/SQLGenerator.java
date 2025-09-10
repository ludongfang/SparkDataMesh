package io.datamesh.core.engine;

import io.datamesh.core.contract.DataContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SQLGenerator {
    
    private static final Logger logger = LoggerFactory.getLogger(SQLGenerator.class);
    
    public String generateCreateTableSQL(String tableName, DataContract.Table tableConfig) {
        logger.info("Generating CREATE TABLE SQL for: {}", tableName);
        
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        
        List<String> columnDefinitions = new ArrayList<>();
        for (Map.Entry<String, DataContract.Field> field : tableConfig.getFields().entrySet()) {
            String columnDef = generateColumnDefinition(field.getKey(), field.getValue());
            columnDefinitions.add("  " + columnDef);
        }
        
        sql.append(String.join(",\n", columnDefinitions));
        sql.append("\n)");
        
        if (tableConfig.getPartitionBy() != null && !tableConfig.getPartitionBy().isEmpty()) {
            sql.append("\nPARTITIONED BY (");
            sql.append(String.join(", ", tableConfig.getPartitionBy()));
            sql.append(")");
        }
        
        if (tableConfig.getClusterBy() != null && !tableConfig.getClusterBy().isEmpty()) {
            sql.append("\nCLUSTERED BY (");
            sql.append(String.join(", ", tableConfig.getClusterBy()));
            sql.append(")");
        }
        
        sql.append("\nSTORED AS PARQUET");
        
        String result = sql.toString();
        logger.debug("Generated CREATE TABLE SQL: {}", result);
        return result;
    }
    
    public String generateTransformationSQL(DataContract.Transformation transformation, 
                                           Map<String, String> tableMapping) {
        logger.info("Generating transformation SQL for type: {}", transformation.getType());
        
        if (transformation.getSql() != null && !transformation.getSql().trim().isEmpty()) {
            return processCustomSQL(transformation.getSql(), tableMapping);
        }
        
        switch (transformation.getType().toLowerCase()) {
            case "extract":
                return generateExtractSQL(transformation, tableMapping);
            case "transform":
                return generateTransformSQL(transformation, tableMapping);
            case "aggregate":
                return generateAggregateSQL(transformation, tableMapping);
            case "join":
                return generateJoinSQL(transformation, tableMapping);
            case "filter":
                return generateFilterSQL(transformation, tableMapping);
            case "union":
                return generateUnionSQL(transformation, tableMapping);
            default:
                throw new IllegalArgumentException("Unsupported transformation type: " + transformation.getType());
        }
    }
    
    private String generateColumnDefinition(String columnName, DataContract.Field field) {
        StringBuilder def = new StringBuilder();
        def.append(columnName).append(" ").append(mapDataType(field));
        
        if (field.getRequired() != null && field.getRequired() && 
            (field.getNullable() == null || !field.getNullable())) {
            def.append(" NOT NULL");
        }
        
        return def.toString();
    }
    
    private String mapDataType(DataContract.Field field) {
        String type = field.getType().toLowerCase();
        
        switch (type) {
            case "string":
                return "STRING";
            case "integer":
                return "INT";
            case "long":
                return "BIGINT";
            case "double":
                return "DOUBLE";
            case "boolean":
                return "BOOLEAN";
            case "timestamp":
                return "TIMESTAMP";
            case "date":
                return "DATE";
            case "decimal":
                if (field.getPrecision() != null && field.getScale() != null) {
                    return String.format("DECIMAL(%d,%d)", field.getPrecision(), field.getScale());
                }
                return "DECIMAL";
            case "array":
                return "ARRAY<STRING>";
            case "struct":
                return "STRUCT<>";
            default:
                return "STRING";
        }
    }
    
    private String generateExtractSQL(DataContract.Transformation transformation, 
                                    Map<String, String> tableMapping) {
        String sourceTable = getSourceTableName(transformation, tableMapping);
        StringBuilder sql = new StringBuilder();
        
        sql.append("SELECT ");
        
        if (transformation.getRules() != null && !transformation.getRules().isEmpty()) {
            List<String> selectColumns = new ArrayList<>();
            for (DataContract.TransformationRule rule : transformation.getRules()) {
                if ("extract".equals(rule.getOperation())) {
                    selectColumns.add(rule.getExpression() + " AS " + rule.getField());
                } else {
                    selectColumns.add(rule.getField());
                }
            }
            sql.append(String.join(", ", selectColumns));
        } else {
            sql.append("*");
        }
        
        sql.append(" FROM ").append(sourceTable);
        
        return sql.toString();
    }
    
    private String generateTransformSQL(DataContract.Transformation transformation, 
                                      Map<String, String> tableMapping) {
        String sourceTable = getSourceTableName(transformation, tableMapping);
        StringBuilder sql = new StringBuilder();
        
        sql.append("SELECT ");
        
        if (transformation.getRules() != null && !transformation.getRules().isEmpty()) {
            List<String> selectColumns = new ArrayList<>();
            for (DataContract.TransformationRule rule : transformation.getRules()) {
                switch (rule.getOperation()) {
                    case "map":
                        selectColumns.add(rule.getExpression() + " AS " + rule.getField());
                        break;
                    case "cast":
                        selectColumns.add("CAST(" + rule.getField() + " AS " + rule.getExpression() + ") AS " + rule.getField());
                        break;
                    case "concat":
                        selectColumns.add("CONCAT(" + rule.getExpression() + ") AS " + rule.getField());
                        break;
                    case "split":
                        selectColumns.add("SPLIT(" + rule.getField() + ", '" + rule.getExpression() + "') AS " + rule.getField());
                        break;
                    default:
                        selectColumns.add(rule.getField());
                }
            }
            sql.append(String.join(", ", selectColumns));
        } else {
            sql.append("*");
        }
        
        sql.append(" FROM ").append(sourceTable);
        
        List<String> conditions = transformation.getRules() != null ? 
            transformation.getRules().stream()
                .filter(rule -> rule.getCondition() != null && !rule.getCondition().isEmpty())
                .map(DataContract.TransformationRule::getCondition)
                .collect(Collectors.toList()) : new ArrayList<>();
        
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        
        return sql.toString();
    }
    
    private String generateAggregateSQL(DataContract.Transformation transformation, 
                                      Map<String, String> tableMapping) {
        String sourceTable = getSourceTableName(transformation, tableMapping);
        StringBuilder sql = new StringBuilder();
        
        sql.append("SELECT ");
        
        List<String> groupByColumns = new ArrayList<>();
        List<String> aggregateColumns = new ArrayList<>();
        
        if (transformation.getRules() != null) {
            for (DataContract.TransformationRule rule : transformation.getRules()) {
                if ("aggregate".equals(rule.getOperation())) {
                    aggregateColumns.add(rule.getExpression() + " AS " + rule.getField());
                } else {
                    groupByColumns.add(rule.getField());
                    aggregateColumns.add(rule.getField());
                }
            }
        }
        
        sql.append(String.join(", ", aggregateColumns));
        sql.append(" FROM ").append(sourceTable);
        
        if (!groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }
        
        return sql.toString();
    }
    
    private String generateJoinSQL(DataContract.Transformation transformation, 
                                 Map<String, String> tableMapping) {
        StringBuilder sql = new StringBuilder();
        
        if (transformation.getSource() instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> sources = (List<String>) transformation.getSource();
            
            if (sources.size() < 2) {
                throw new IllegalArgumentException("Join transformation requires at least 2 source tables");
            }
            
            sql.append("SELECT * FROM ");
            sql.append(tableMapping.get(sources.get(0)));
            
            for (int i = 1; i < sources.size(); i++) {
                sql.append(" JOIN ").append(tableMapping.get(sources.get(i)));
                
                if (transformation.getRules() != null && i <= transformation.getRules().size()) {
                    DataContract.TransformationRule joinRule = transformation.getRules().get(i - 1);
                    if (joinRule.getCondition() != null) {
                        sql.append(" ON ").append(joinRule.getCondition());
                    }
                }
            }
        }
        
        return sql.toString();
    }
    
    private String generateFilterSQL(DataContract.Transformation transformation, 
                                   Map<String, String> tableMapping) {
        String sourceTable = getSourceTableName(transformation, tableMapping);
        StringBuilder sql = new StringBuilder();
        
        sql.append("SELECT * FROM ").append(sourceTable);
        
        List<String> conditions = transformation.getRules() != null ? 
            transformation.getRules().stream()
                .filter(rule -> "filter".equals(rule.getOperation()) && rule.getCondition() != null)
                .map(DataContract.TransformationRule::getCondition)
                .collect(Collectors.toList()) : new ArrayList<>();
        
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        
        return sql.toString();
    }
    
    private String generateUnionSQL(DataContract.Transformation transformation, 
                                  Map<String, String> tableMapping) {
        if (transformation.getSource() instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> sources = (List<String>) transformation.getSource();
            
            return sources.stream()
                .map(source -> "SELECT * FROM " + tableMapping.get(source))
                .collect(Collectors.joining(" UNION ALL "));
        } else {
            throw new IllegalArgumentException("Union transformation requires multiple source tables");
        }
    }
    
    private String getSourceTableName(DataContract.Transformation transformation, 
                                    Map<String, String> tableMapping) {
        if (transformation.getSource() instanceof String) {
            String sourceTable = (String) transformation.getSource();
            return tableMapping.getOrDefault(sourceTable, sourceTable);
        } else if (transformation.getSource() instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> sources = (List<String>) transformation.getSource();
            if (!sources.isEmpty()) {
                return tableMapping.getOrDefault(sources.get(0), sources.get(0));
            }
        }
        throw new IllegalArgumentException("Invalid source configuration for transformation");
    }
    
    private String processCustomSQL(String sql, Map<String, String> tableMapping) {
        String processedSQL = sql;
        
        for (Map.Entry<String, String> mapping : tableMapping.entrySet()) {
            String placeholder = "${" + mapping.getKey() + "}";
            processedSQL = processedSQL.replace(placeholder, mapping.getValue());
        }
        
        return processedSQL;
    }
}