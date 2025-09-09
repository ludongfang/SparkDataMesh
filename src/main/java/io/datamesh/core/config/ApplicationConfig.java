package io.datamesh.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ApplicationConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(ApplicationConfig.class);
    
    private String sparkAppName;
    private String sparkMaster;
    private Map<String, String> sparkConfigs;
    private String defaultDataPath;
    private boolean enableValidation;
    private int maxRetries;
    private long timeoutMs;
    
    public ApplicationConfig() {
        this.sparkConfigs = new HashMap<>();
        this.enableValidation = true;
        this.maxRetries = 3;
        this.timeoutMs = 300000; // 5 minutes
    }
    
    public static ApplicationConfig loadFromFile(String configFilePath) {
        logger.info("Loading configuration from file: {}", configFilePath);
        
        Config config = ConfigFactory.parseFile(new File(configFilePath));
        return fromConfig(config);
    }
    
    public static ApplicationConfig loadDefault() {
        logger.info("Loading default configuration");
        
        Config config = ConfigFactory.load();
        return fromConfig(config);
    }
    
    public static ApplicationConfig createDefault() {
        logger.info("Creating default configuration");
        
        ApplicationConfig appConfig = new ApplicationConfig();
        appConfig.setSparkAppName("SparkDataFlow");
        appConfig.setSparkMaster("local[*]");
        appConfig.setDefaultDataPath("/tmp/sparkdataflow");
        appConfig.setEnableValidation(true);
        appConfig.setMaxRetries(3);
        appConfig.setTimeoutMs(300000);
        
        Map<String, String> defaultSparkConfigs = new HashMap<>();
        defaultSparkConfigs.put("spark.sql.adaptive.enabled", "true");
        defaultSparkConfigs.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
        defaultSparkConfigs.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        appConfig.setSparkConfigs(defaultSparkConfigs);
        
        return appConfig;
    }
    
    private static ApplicationConfig fromConfig(Config config) {
        ApplicationConfig appConfig = new ApplicationConfig();
        
        try {
            if (config.hasPath("spark.appName")) {
                appConfig.setSparkAppName(config.getString("spark.appName"));
            } else {
                appConfig.setSparkAppName("SparkDataFlow");
            }
            
            if (config.hasPath("spark.master")) {
                appConfig.setSparkMaster(config.getString("spark.master"));
            }
            
            if (config.hasPath("data.defaultPath")) {
                appConfig.setDefaultDataPath(config.getString("data.defaultPath"));
            } else {
                appConfig.setDefaultDataPath("/tmp/sparkdataflow");
            }
            
            if (config.hasPath("validation.enabled")) {
                appConfig.setEnableValidation(config.getBoolean("validation.enabled"));
            }
            
            if (config.hasPath("execution.maxRetries")) {
                appConfig.setMaxRetries(config.getInt("execution.maxRetries"));
            }
            
            if (config.hasPath("execution.timeoutMs")) {
                appConfig.setTimeoutMs(config.getLong("execution.timeoutMs"));
            }
            
            Map<String, String> sparkConfigs = new HashMap<>();
            if (config.hasPath("spark.configs")) {
                Config sparkConfig = config.getConfig("spark.configs");
                sparkConfig.entrySet().forEach(entry -> {
                    sparkConfigs.put(entry.getKey(), entry.getValue().unwrapped().toString());
                });
            }
            
            sparkConfigs.put("spark.sql.adaptive.enabled", "true");
            sparkConfigs.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
            sparkConfigs.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            
            appConfig.setSparkConfigs(sparkConfigs);
            
        } catch (Exception e) {
            logger.warn("Error parsing configuration, using defaults where possible", e);
        }
        
        return appConfig;
    }
    
    public String getSparkAppName() {
        return sparkAppName;
    }
    
    public void setSparkAppName(String sparkAppName) {
        this.sparkAppName = sparkAppName;
    }
    
    public String getSparkMaster() {
        return sparkMaster;
    }
    
    public void setSparkMaster(String sparkMaster) {
        this.sparkMaster = sparkMaster;
    }
    
    public Map<String, String> getSparkConfigs() {
        return sparkConfigs;
    }
    
    public void setSparkConfigs(Map<String, String> sparkConfigs) {
        this.sparkConfigs = sparkConfigs;
    }
    
    public String getDefaultDataPath() {
        return defaultDataPath;
    }
    
    public void setDefaultDataPath(String defaultDataPath) {
        this.defaultDataPath = defaultDataPath;
    }
    
    public boolean isEnableValidation() {
        return enableValidation;
    }
    
    public void setEnableValidation(boolean enableValidation) {
        this.enableValidation = enableValidation;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public long getTimeoutMs() {
        return timeoutMs;
    }
    
    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
}