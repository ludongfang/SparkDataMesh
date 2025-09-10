package io.datamesh.core.validation;

import io.datamesh.core.contract.DataContract;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public class DataValidator {
    
    private static final Logger logger = LoggerFactory.getLogger(DataValidator.class);
    
    public boolean validateExpectation(Dataset<Row> dataset, DataContract.Expectation expectation) {
        logger.info("Validating expectation: {} for table: {}", expectation.getType(), expectation.getTable());
        
        try {
            switch (expectation.getType().toLowerCase()) {
                case "not_null":
                    return validateNotNull(dataset, expectation);
                case "unique":
                    return validateUnique(dataset, expectation);
                case "range":
                    return validateRange(dataset, expectation);
                case "pattern":
                    return validatePattern(dataset, expectation);
                case "custom":
                    return validateCustom(dataset, expectation);
                case "freshness":
                    return validateFreshness(dataset, expectation);
                case "completeness":
                    return validateCompleteness(dataset, expectation);
                default:
                    logger.warn("Unknown expectation type: {}", expectation.getType());
                    return true;
            }
        } catch (Exception e) {
            logger.error("Error validating expectation: {} for table: {}", 
                        expectation.getType(), expectation.getTable(), e);
            return false;
        }
    }
    
    private boolean validateNotNull(Dataset<Row> dataset, DataContract.Expectation expectation) {
        if (expectation.getField() == null) {
            logger.error("Field name is required for not_null validation");
            return false;
        }
        
        long nullCount = dataset.filter(col(expectation.getField()).isNull()).count();
        long totalCount = dataset.count();
        
        logger.info("Not null validation for field {}: {} null values out of {} total", 
                   expectation.getField(), nullCount, totalCount);
        
        if (nullCount > 0) {
            logger.warn("Found {} null values in field: {}", nullCount, expectation.getField());
            return false;
        }
        
        return true;
    }
    
    private boolean validateUnique(Dataset<Row> dataset, DataContract.Expectation expectation) {
        if (expectation.getField() == null) {
            logger.error("Field name is required for unique validation");
            return false;
        }
        
        long totalCount = dataset.count();
        long distinctCount = dataset.select(expectation.getField()).distinct().count();
        
        logger.info("Unique validation for field {}: {} distinct values out of {} total", 
                   expectation.getField(), distinctCount, totalCount);
        
        if (distinctCount != totalCount) {
            logger.warn("Found duplicate values in field: {}. Distinct: {}, Total: {}", 
                       expectation.getField(), distinctCount, totalCount);
            return false;
        }
        
        return true;
    }
    
    private boolean validateRange(Dataset<Row> dataset, DataContract.Expectation expectation) {
        if (expectation.getField() == null || expectation.getExpression() == null) {
            logger.error("Field name and expression are required for range validation");
            return false;
        }
        
        try {
            String[] rangeParts = expectation.getExpression().split(",");
            if (rangeParts.length != 2) {
                logger.error("Range expression must be in format 'min,max'");
                return false;
            }
            
            double min = Double.parseDouble(rangeParts[0].trim());
            double max = Double.parseDouble(rangeParts[1].trim());
            
            long outOfRangeCount = dataset
                .filter(col(expectation.getField()).lt(min).or(col(expectation.getField()).gt(max)))
                .count();
            
            long totalCount = dataset.count();
            double outOfRangePercentage = (double) outOfRangeCount / totalCount * 100;
            
            logger.info("Range validation for field {}: {} out of range values ({:.2f}%)", 
                       expectation.getField(), outOfRangeCount, outOfRangePercentage);
            
            double threshold = expectation.getThreshold() != null ? expectation.getThreshold() : 0.0;
            
            if (outOfRangePercentage > threshold) {
                logger.warn("Range validation failed for field: {}. Out of range: {:.2f}%, Threshold: {:.2f}%", 
                           expectation.getField(), outOfRangePercentage, threshold);
                return false;
            }
            
            return true;
            
        } catch (NumberFormatException e) {
            logger.error("Invalid range format in expression: {}", expectation.getExpression(), e);
            return false;
        }
    }
    
    private boolean validatePattern(Dataset<Row> dataset, DataContract.Expectation expectation) {
        if (expectation.getField() == null || expectation.getExpression() == null) {
            logger.error("Field name and pattern expression are required for pattern validation");
            return false;
        }
        
        long invalidPatternCount = dataset
            .filter(not(col(expectation.getField()).rlike(expectation.getExpression())))
            .count();
        
        long totalCount = dataset.count();
        double invalidPercentage = (double) invalidPatternCount / totalCount * 100;
        
        logger.info("Pattern validation for field {}: {} invalid patterns ({:.2f}%)", 
                   expectation.getField(), invalidPatternCount, invalidPercentage);
        
        double threshold = expectation.getThreshold() != null ? expectation.getThreshold() : 0.0;
        
        if (invalidPercentage > threshold) {
            logger.warn("Pattern validation failed for field: {}. Invalid: {:.2f}%, Threshold: {:.2f}%", 
                       expectation.getField(), invalidPercentage, threshold);
            return false;
        }
        
        return true;
    }
    
    private boolean validateCustom(Dataset<Row> dataset, DataContract.Expectation expectation) {
        if (expectation.getExpression() == null) {
            logger.error("Expression is required for custom validation");
            return false;
        }
        
        try {
            long violationCount = dataset.filter(not(expr(expectation.getExpression()))).count();
            long totalCount = dataset.count();
            double violationPercentage = (double) violationCount / totalCount * 100;
            
            logger.info("Custom validation: {} violations ({:.2f}%)", 
                       violationCount, violationPercentage);
            
            double threshold = expectation.getThreshold() != null ? expectation.getThreshold() : 0.0;
            
            if (violationPercentage > threshold) {
                logger.warn("Custom validation failed. Violations: {:.2f}%, Threshold: {:.2f}%", 
                           violationPercentage, threshold);
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            logger.error("Error in custom validation expression: {}", expectation.getExpression(), e);
            return false;
        }
    }
    
    private boolean validateFreshness(Dataset<Row> dataset, DataContract.Expectation expectation) {
        if (expectation.getField() == null || expectation.getExpression() == null) {
            logger.error("Field name and time expression are required for freshness validation");
            return false;
        }
        
        try {
            int maxAgeHours = Integer.parseInt(expectation.getExpression());
            
            Dataset<Row> freshData = dataset.filter(
                col(expectation.getField()).gt(
                    expr("current_timestamp() - interval " + maxAgeHours + " hours")
                )
            );
            
            long freshCount = freshData.count();
            long totalCount = dataset.count();
            double freshnessPercentage = (double) freshCount / totalCount * 100;
            
            logger.info("Freshness validation for field {}: {:.2f}% of data is fresh", 
                       expectation.getField(), freshnessPercentage);
            
            double threshold = expectation.getThreshold() != null ? expectation.getThreshold() : 100.0;
            
            if (freshnessPercentage < threshold) {
                logger.warn("Freshness validation failed for field: {}. Fresh: {:.2f}%, Threshold: {:.2f}%", 
                           expectation.getField(), freshnessPercentage, threshold);
                return false;
            }
            
            return true;
            
        } catch (NumberFormatException e) {
            logger.error("Invalid time expression for freshness validation: {}", expectation.getExpression(), e);
            return false;
        }
    }
    
    private boolean validateCompleteness(Dataset<Row> dataset, DataContract.Expectation expectation) {
        if (expectation.getField() == null) {
            logger.error("Field name is required for completeness validation");
            return false;
        }
        
        long nonNullCount = dataset.filter(col(expectation.getField()).isNotNull()).count();
        long totalCount = dataset.count();
        double completenessPercentage = (double) nonNullCount / totalCount * 100;
        
        logger.info("Completeness validation for field {}: {:.2f}% complete", 
                   expectation.getField(), completenessPercentage);
        
        double threshold = expectation.getThreshold() != null ? expectation.getThreshold() : 100.0;
        
        if (completenessPercentage < threshold) {
            logger.warn("Completeness validation failed for field: {}. Complete: {:.2f}%, Threshold: {:.2f}%", 
                       expectation.getField(), completenessPercentage, threshold);
            return false;
        }
        
        return true;
    }
}