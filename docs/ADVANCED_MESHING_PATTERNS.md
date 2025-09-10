# Advanced Data Meshing Patterns

This document outlines advanced patterns and techniques for using the SparkDataMesh Data Contract DSL to implement sophisticated data meshing scenarios.

## Table of Contents

1. [Multi-Source Data Meshing](#multi-source-data-meshing)
2. [Temporal Data Patterns](#temporal-data-patterns)
3. [Feature Engineering Patterns](#feature-engineering-patterns)
4. [Data Quality Patterns](#data-quality-patterns)
5. [Performance Optimization Patterns](#performance-optimization-patterns)
6. [Real-Time Streaming Patterns](#real-time-streaming-patterns)
7. [Domain-Driven Design Patterns](#domain-driven-design-patterns)
8. [Testing and Validation Patterns](#testing-and-validation-patterns)

## Multi-Source Data Meshing

### Pattern 1: Star Schema Transformation

Transform multiple related sources into a dimensional model:

```yaml
transformations:
  sales_fact_table:
    type: "join"
    description: "Create sales fact table from multiple sources"
    source: ["transactions", "products", "customers", "stores", "time_dim"]
    target: "sales_facts"
    sql: |
      SELECT 
        t.transaction_id,
        t.transaction_date,
        td.date_key,
        c.customer_key,
        p.product_key,
        s.store_key,
        
        -- Measures
        t.quantity,
        t.unit_price,
        t.total_amount,
        t.discount_amount,
        (t.total_amount - t.discount_amount) as net_amount,
        
        -- Calculated measures
        CASE 
          WHEN p.cost_price > 0 
          THEN (t.unit_price - p.cost_price) * t.quantity 
          ELSE 0 
        END as gross_profit,
        
        -- Flags
        (t.total_amount >= 1000) as is_high_value_transaction,
        (c.loyalty_tier = 'premium') as is_premium_customer
        
      FROM transactions t
      JOIN time_dimensions td ON DATE(t.transaction_date) = td.date_value
      JOIN customers c ON t.customer_id = c.customer_id
      JOIN products p ON t.product_id = p.product_id
      JOIN stores s ON t.store_id = s.store_id
      WHERE t.transaction_date >= CURRENT_DATE - INTERVAL 2 YEARS
```

### Pattern 2: Event Stream Aggregation

Aggregate multiple event streams with different granularities:

```yaml
transformations:
  user_activity_rollup:
    type: "aggregate"
    description: "Multi-level aggregation of user activities"
    source: ["page_views", "clicks", "purchases", "searches"]
    target: "user_activity_summary"
    sql: |
      WITH hourly_aggregates AS (
        SELECT 
          user_id,
          DATE_TRUNC('hour', event_timestamp) as hour_window,
          event_type,
          COUNT(*) as event_count,
          COUNT(DISTINCT session_id) as unique_sessions
        FROM (
          SELECT user_id, event_timestamp, 'page_view' as event_type, session_id FROM page_views
          UNION ALL
          SELECT user_id, event_timestamp, 'click' as event_type, session_id FROM clicks
          UNION ALL
          SELECT user_id, event_timestamp, 'purchase' as event_type, session_id FROM purchases
          UNION ALL
          SELECT user_id, event_timestamp, 'search' as event_type, session_id FROM searches
        ) all_events
        WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
        GROUP BY user_id, DATE_TRUNC('hour', event_timestamp), event_type
      ),
      
      daily_summary AS (
        SELECT 
          user_id,
          DATE(hour_window) as activity_date,
          SUM(CASE WHEN event_type = 'page_view' THEN event_count ELSE 0 END) as daily_page_views,
          SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) as daily_clicks,
          SUM(CASE WHEN event_type = 'purchase' THEN event_count ELSE 0 END) as daily_purchases,
          SUM(CASE WHEN event_type = 'search' THEN event_count ELSE 0 END) as daily_searches,
          SUM(unique_sessions) as daily_sessions,
          COUNT(DISTINCT hour_window) as active_hours
        FROM hourly_aggregates
        GROUP BY user_id, DATE(hour_window)
      )
      
      SELECT 
        user_id,
        activity_date,
        daily_page_views,
        daily_clicks,
        daily_purchases,
        daily_searches,
        daily_sessions,
        active_hours,
        
        -- Engagement metrics
        CASE 
          WHEN daily_page_views > 0 
          THEN daily_clicks / daily_page_views 
          ELSE 0 
        END as daily_ctr,
        
        CASE 
          WHEN daily_sessions > 0 
          THEN (daily_page_views + daily_clicks) / daily_sessions 
          ELSE 0 
        END as avg_events_per_session,
        
        -- Activity intensity
        CASE 
          WHEN active_hours >= 8 THEN 'high'
          WHEN active_hours >= 4 THEN 'medium'
          WHEN active_hours >= 1 THEN 'low'
          ELSE 'minimal'
        END as activity_intensity
        
      FROM daily_summary
```

## Temporal Data Patterns

### Pattern 3: Slowly Changing Dimensions (SCD Type 2)

Handle historical changes in dimension data:

```yaml
transformations:
  customer_scd_type2:
    type: "transform"
    description: "Implement SCD Type 2 for customer changes"
    source: ["customers_current", "customers_staging"]
    target: "customers_history"
    sql: |
      WITH current_customers AS (
        SELECT 
          customer_id,
          customer_name,
          email,
          address,
          phone,
          segment,
          CURRENT_TIMESTAMP as effective_start_date,
          CAST('9999-12-31' AS TIMESTAMP) as effective_end_date,
          true as is_current,
          HASH(customer_name, email, address, phone, segment) as record_hash
        FROM customers_staging
      ),
      
      changed_customers AS (
        SELECT 
          c.customer_id,
          c.customer_name,
          c.email,
          c.address,
          c.phone,
          c.segment,
          c.record_hash as new_hash,
          h.record_hash as old_hash,
          h.effective_start_date as old_start_date
        FROM current_customers c
        JOIN customers_history h ON c.customer_id = h.customer_id AND h.is_current = true
        WHERE c.record_hash != h.record_hash
      ),
      
      -- Close old records
      updated_history AS (
        SELECT 
          h.customer_id,
          h.customer_name,
          h.email,
          h.address,
          h.phone,
          h.segment,
          h.effective_start_date,
          CASE 
            WHEN cc.customer_id IS NOT NULL 
            THEN CURRENT_TIMESTAMP 
            ELSE h.effective_end_date 
          END as effective_end_date,
          CASE 
            WHEN cc.customer_id IS NOT NULL 
            THEN false 
            ELSE h.is_current 
          END as is_current,
          h.record_hash
        FROM customers_history h
        LEFT JOIN changed_customers cc ON h.customer_id = cc.customer_id
      ),
      
      -- New records for changed customers
      new_records AS (
        SELECT 
          customer_id,
          customer_name,
          email,
          address,
          phone,
          segment,
          CURRENT_TIMESTAMP as effective_start_date,
          CAST('9999-12-31' AS TIMESTAMP) as effective_end_date,
          true as is_current,
          new_hash as record_hash
        FROM changed_customers
      ),
      
      -- Completely new customers
      new_customers AS (
        SELECT 
          c.customer_id,
          c.customer_name,
          c.email,
          c.address,
          c.phone,
          c.segment,
          c.effective_start_date,
          c.effective_end_date,
          c.is_current,
          c.record_hash
        FROM current_customers c
        LEFT JOIN customers_history h ON c.customer_id = h.customer_id
        WHERE h.customer_id IS NULL
      )
      
      SELECT * FROM updated_history
      UNION ALL
      SELECT * FROM new_records
      UNION ALL
      SELECT * FROM new_customers
```

### Pattern 4: Time-Series Feature Engineering

Create time-based features with windowing functions:

```yaml
transformations:
  time_series_features:
    type: "transform"
    description: "Generate time-series features with multiple window sizes"
    source: "user_transactions"
    target: "user_temporal_features"
    sql: |
      WITH daily_transactions AS (
        SELECT 
          user_id,
          DATE(transaction_date) as transaction_date,
          COUNT(*) as daily_transaction_count,
          SUM(amount) as daily_transaction_amount,
          AVG(amount) as daily_avg_amount,
          MIN(amount) as daily_min_amount,
          MAX(amount) as daily_max_amount,
          STDDEV(amount) as daily_amount_stddev
        FROM user_transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL 365 DAYS
        GROUP BY user_id, DATE(transaction_date)
      ),
      
      windowed_features AS (
        SELECT 
          user_id,
          transaction_date,
          daily_transaction_count,
          daily_transaction_amount,
          
          -- 7-day rolling windows
          AVG(daily_transaction_count) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) as avg_transactions_7d,
          
          SUM(daily_transaction_amount) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) as total_amount_7d,
          
          -- 30-day rolling windows
          AVG(daily_transaction_count) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as avg_transactions_30d,
          
          SUM(daily_transaction_amount) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as total_amount_30d,
          
          -- 90-day rolling windows
          AVG(daily_transaction_count) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
          ) as avg_transactions_90d,
          
          -- Lag features
          LAG(daily_transaction_amount, 1) OVER (
            PARTITION BY user_id ORDER BY transaction_date
          ) as prev_day_amount,
          
          LAG(daily_transaction_amount, 7) OVER (
            PARTITION BY user_id ORDER BY transaction_date
          ) as same_day_last_week_amount,
          
          -- Trend features
          (daily_transaction_amount - LAG(daily_transaction_amount, 7) OVER (
            PARTITION BY user_id ORDER BY transaction_date
          )) / NULLIF(LAG(daily_transaction_amount, 7) OVER (
            PARTITION BY user_id ORDER BY transaction_date
          ), 0) as week_over_week_growth,
          
          -- Seasonality features
          EXTRACT(DAYOFWEEK FROM transaction_date) as day_of_week,
          EXTRACT(MONTH FROM transaction_date) as month_of_year,
          EXTRACT(QUARTER FROM transaction_date) as quarter_of_year,
          
          -- Volatility features
          STDDEV(daily_transaction_amount) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as amount_volatility_30d
          
        FROM daily_transactions
      )
      
      SELECT 
        user_id,
        transaction_date,
        daily_transaction_count,
        daily_transaction_amount,
        avg_transactions_7d,
        total_amount_7d,
        avg_transactions_30d,
        total_amount_30d,
        avg_transactions_90d,
        prev_day_amount,
        same_day_last_week_amount,
        week_over_week_growth,
        day_of_week,
        month_of_year,
        quarter_of_year,
        amount_volatility_30d,
        
        -- Derived features
        CASE 
          WHEN avg_transactions_7d > avg_transactions_30d * 1.2 THEN 'increasing'
          WHEN avg_transactions_7d < avg_transactions_30d * 0.8 THEN 'decreasing'
          ELSE 'stable'
        END as transaction_trend,
        
        CASE 
          WHEN amount_volatility_30d > total_amount_30d * 0.5 THEN 'high'
          WHEN amount_volatility_30d > total_amount_30d * 0.2 THEN 'medium'
          ELSE 'low'
        END as spending_volatility_category
        
      FROM windowed_features
```

## Feature Engineering Patterns

### Pattern 5: Categorical Feature Engineering

Transform categorical data for machine learning:

```yaml
transformations:
  categorical_feature_engineering:
    type: "transform"
    description: "Advanced categorical feature engineering"
    source: ["user_profiles", "user_transactions", "products"]
    target: "user_categorical_features"
    sql: |
      WITH user_category_stats AS (
        SELECT 
          up.user_id,
          up.age_group,
          up.gender,
          up.country,
          up.occupation,
          
          -- Category interaction frequencies
          COUNT(DISTINCT p.category_id) as unique_categories_purchased,
          MODE(p.category_id) as favorite_category,
          
          -- Category spending patterns
          COLLECT_LIST(STRUCT(
            p.category_id,
            SUM(ut.amount) as category_spend,
            COUNT(*) as category_frequency
          )) as category_spending_patterns,
          
          -- Brand preferences
          COUNT(DISTINCT p.brand) as unique_brands_purchased,
          MODE(p.brand) as favorite_brand,
          
          -- Price tier preferences
          AVG(CASE 
            WHEN ut.amount <= 50 THEN 1
            WHEN ut.amount <= 200 THEN 2
            WHEN ut.amount <= 500 THEN 3
            ELSE 4
          END) as avg_price_tier_preference
          
        FROM user_profiles up
        LEFT JOIN user_transactions ut ON up.user_id = ut.user_id
        LEFT JOIN products p ON ut.product_id = p.product_id
        WHERE ut.transaction_date >= CURRENT_DATE - INTERVAL 365 DAYS
        GROUP BY up.user_id, up.age_group, up.gender, up.country, up.occupation
      ),
      
      category_encoding AS (
        SELECT 
          user_id,
          
          -- One-hot encoding for age groups
          (age_group = '18-24') as age_18_24,
          (age_group = '25-34') as age_25_34,
          (age_group = '35-44') as age_35_44,
          (age_group = '45-54') as age_45_54,
          (age_group = '55-64') as age_55_64,
          (age_group = '65+') as age_65_plus,
          
          -- Gender encoding
          (gender = 'M') as is_male,
          (gender = 'F') as is_female,
          
          -- Country groupings
          CASE 
            WHEN country IN ('US', 'CA', 'MX') THEN 'north_america'
            WHEN country IN ('GB', 'DE', 'FR', 'IT', 'ES') THEN 'europe'
            WHEN country IN ('JP', 'CN', 'KR', 'IN') THEN 'asia'
            ELSE 'other'
          END as region_group,
          
          -- Category diversity score
          unique_categories_purchased / 20.0 as category_diversity_score,
          
          -- Brand loyalty score  
          CASE 
            WHEN unique_brands_purchased = 0 THEN 0
            ELSE 1.0 / unique_brands_purchased
          END as brand_loyalty_score,
          
          -- Target encoding for favorite category (simplified)
          CASE favorite_category
            WHEN 'electronics' THEN 0.85
            WHEN 'clothing' THEN 0.72
            WHEN 'books' THEN 0.68
            WHEN 'home' THEN 0.75
            ELSE 0.60
          END as favorite_category_value_score,
          
          -- Ordinal encoding for price preference
          avg_price_tier_preference,
          
          favorite_category,
          favorite_brand
          
        FROM user_category_stats
      )
      
      SELECT 
        user_id,
        age_18_24,
        age_25_34,
        age_35_44,
        age_45_54,
        age_55_64,
        age_65_plus,
        is_male,
        is_female,
        region_group,
        category_diversity_score,
        brand_loyalty_score,
        favorite_category_value_score,
        avg_price_tier_preference,
        favorite_category,
        favorite_brand,
        
        -- Interaction features
        (category_diversity_score * brand_loyalty_score) as diversity_loyalty_interaction,
        (avg_price_tier_preference * favorite_category_value_score) as price_category_interaction
        
      FROM category_encoding
```

### Pattern 6: Anomaly Detection Features

Generate features for anomaly detection:

```yaml
transformations:
  anomaly_detection_features:
    type: "transform"
    description: "Generate features for anomaly detection models"
    source: "user_transactions"
    target: "anomaly_features"
    sql: |
      WITH user_transaction_patterns AS (
        SELECT 
          user_id,
          transaction_id,
          transaction_date,
          amount,
          merchant_category,
          location_country,
          payment_method,
          
          -- Time-based features
          EXTRACT(HOUR FROM transaction_date) as hour_of_day,
          EXTRACT(DAYOFWEEK FROM transaction_date) as day_of_week,
          EXTRACT(DAY FROM transaction_date) as day_of_month,
          
          -- User historical statistics (30-day window)
          AVG(amount) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as user_avg_amount_30d,
          
          STDDEV(amount) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as user_stddev_amount_30d,
          
          COUNT(*) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as user_transaction_count_30d,
          
          -- Location patterns
          MODE(location_country) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as user_primary_country,
          
          COUNT(DISTINCT location_country) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as user_country_diversity_30d,
          
          -- Merchant patterns
          COUNT(DISTINCT merchant_category) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as user_merchant_diversity_30d,
          
          -- Payment method patterns
          MODE(payment_method) OVER (
            PARTITION BY user_id 
            ORDER BY transaction_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) as user_primary_payment_method,
          
          -- Time since last transaction
          (UNIX_TIMESTAMP(transaction_date) - 
           UNIX_TIMESTAMP(LAG(transaction_date, 1) OVER (
             PARTITION BY user_id ORDER BY transaction_date
           ))) / 3600.0 as hours_since_last_transaction
          
        FROM user_transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL 60 DAYS
      ),
      
      global_statistics AS (
        SELECT 
          merchant_category,
          hour_of_day,
          AVG(amount) as global_avg_amount_by_merchant_hour,
          STDDEV(amount) as global_stddev_amount_by_merchant_hour,
          COUNT(*) as global_count_by_merchant_hour
        FROM user_transaction_patterns
        GROUP BY merchant_category, hour_of_day
      )
      
      SELECT 
        utp.user_id,
        utp.transaction_id,
        utp.transaction_date,
        utp.amount,
        utp.merchant_category,
        utp.location_country,
        utp.payment_method,
        
        -- Deviation scores
        CASE 
          WHEN utp.user_stddev_amount_30d > 0 
          THEN ABS(utp.amount - utp.user_avg_amount_30d) / utp.user_stddev_amount_30d
          ELSE 0
        END as amount_z_score,
        
        CASE 
          WHEN gs.global_stddev_amount_by_merchant_hour > 0
          THEN ABS(utp.amount - gs.global_avg_amount_by_merchant_hour) / gs.global_stddev_amount_by_merchant_hour
          ELSE 0
        END as global_amount_z_score,
        
        -- Behavioral anomalies
        (utp.location_country != utp.user_primary_country) as is_foreign_transaction,
        (utp.payment_method != utp.user_primary_payment_method) as is_unusual_payment_method,
        
        -- Time-based anomalies
        CASE 
          WHEN utp.hour_of_day BETWEEN 2 AND 5 THEN true  -- Late night transactions
          ELSE false
        END as is_unusual_hour,
        
        CASE 
          WHEN utp.hours_since_last_transaction < 0.1 THEN true  -- Very rapid transactions
          ELSE false
        END as is_rapid_transaction,
        
        CASE 
          WHEN utp.hours_since_last_transaction > 168 THEN true  -- First transaction in a week
          ELSE false
        END as is_long_gap_transaction,
        
        -- Frequency anomalies
        CASE 
          WHEN utp.user_transaction_count_30d > 100 THEN true  -- Very high frequency
          ELSE false
        END as is_high_frequency_user,
        
        -- Diversity anomalies
        CASE 
          WHEN utp.user_country_diversity_30d > 5 THEN true  -- Multiple countries
          ELSE false
        END as is_high_location_diversity,
        
        CASE 
          WHEN utp.user_merchant_diversity_30d > 20 THEN true  -- Many different merchants
          ELSE false
        END as is_high_merchant_diversity,
        
        -- Amount percentiles within user history
        PERCENT_RANK() OVER (
          PARTITION BY utp.user_id ORDER BY utp.amount
        ) as amount_percentile_within_user,
        
        -- Raw features for model
        utp.user_avg_amount_30d,
        utp.user_stddev_amount_30d,
        utp.user_transaction_count_30d,
        utp.user_country_diversity_30d,
        utp.user_merchant_diversity_30d,
        utp.hours_since_last_transaction,
        
        -- Composite anomaly score
        (
          CASE WHEN utp.user_stddev_amount_30d > 0 
               THEN ABS(utp.amount - utp.user_avg_amount_30d) / utp.user_stddev_amount_30d
               ELSE 0 END * 0.3 +
          (utp.location_country != utp.user_primary_country)::int * 0.2 +
          (utp.payment_method != utp.user_primary_payment_method)::int * 0.1 +
          CASE WHEN utp.hour_of_day BETWEEN 2 AND 5 THEN 0.2 ELSE 0 END +
          CASE WHEN utp.hours_since_last_transaction < 0.1 THEN 0.2 ELSE 0 END
        ) as composite_anomaly_score
        
      FROM user_transaction_patterns utp
      LEFT JOIN global_statistics gs ON utp.merchant_category = gs.merchant_category 
                                    AND utp.hour_of_day = gs.hour_of_day
```

## Data Quality Patterns

### Pattern 7: Comprehensive Data Profiling

Generate data quality profiles across multiple dimensions:

```yaml
quality:
  expectations:
    # Completeness checks
    - type: "completeness"
      table: "user_profiles"
      field: "email"
      description: "Email completeness should be high"
      threshold: 95.0
      severity: "warning"
    
    - type: "completeness"
      table: "transactions"
      field: "amount"
      description: "Transaction amount must be complete"
      threshold: 100.0
      severity: "error"
    
    # Uniqueness checks
    - type: "unique"
      table: "user_profiles"
      field: "user_id"
      description: "User IDs must be unique"
      severity: "error"
    
    - type: "unique"
      table: "transactions"
      field: "transaction_id"
      description: "Transaction IDs must be unique"
      severity: "error"
    
    # Range validation
    - type: "range"
      table: "transactions"
      field: "amount"
      description: "Transaction amounts should be reasonable"
      expression: "0,1000000"
      threshold: 2.0  # Allow 2% outliers
      severity: "warning"
    
    - type: "range"
      table: "user_profiles"
      field: "age"
      description: "User ages should be realistic"
      expression: "13,120"
      threshold: 1.0
      severity: "error"
    
    # Pattern validation
    - type: "pattern"
      table: "user_profiles"
      field: "email"
      description: "Email format validation"
      expression: "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"
      threshold: 3.0  # Allow 3% format issues
      severity: "warning"
    
    - type: "pattern"
      table: "transactions"
      field: "transaction_id"
      description: "Transaction ID format"
      expression: "^TXN-\\d{8}-[A-Z0-9]{6}$"
      threshold: 1.0
      severity: "error"
    
    # Custom business logic validation
    - type: "custom"
      table: "transactions"
      description: "High-value transactions should have fraud checks"
      expression: |
        CASE 
          WHEN amount >= 10000 AND fraud_check_status IS NULL 
          THEN 0 
          ELSE 1 
        END
      threshold: 100.0
      severity: "error"
    
    - type: "custom"
      table: "user_profiles"
      description: "Premium users should have complete profiles"
      expression: |
        CASE 
          WHEN subscription_tier = 'premium' 
               AND (phone IS NULL OR address IS NULL)
          THEN 0 
          ELSE 1 
        END
      threshold: 95.0
      severity: "warning"
    
    # Cross-table consistency
    - type: "custom"
      table: "transactions"
      description: "All transactions should have valid user references"
      expression: |
        CASE 
          WHEN user_id NOT IN (SELECT user_id FROM user_profiles)
          THEN 0 
          ELSE 1 
        END
      threshold: 100.0
      severity: "error"
    
    # Freshness validation
    - type: "freshness"
      table: "transactions"
      field: "transaction_date"
      description: "Transaction data should be fresh"
      expression: "2"  # 2 hours
      threshold: 95.0
      severity: "warning"
    
    # Distribution validation
    - type: "custom"
      table: "transactions"
      description: "Payment method distribution should be stable"
      expression: |
        CASE 
          WHEN payment_method = 'credit_card' 
               AND COUNT(*) OVER (PARTITION BY DATE(transaction_date)) / 
                   SUM(COUNT(*)) OVER (PARTITION BY DATE(transaction_date)) < 0.4
          THEN 0 
          ELSE 1 
        END
      threshold: 90.0
      severity: "warning"
```

## Performance Optimization Patterns

### Pattern 8: Partitioning and Clustering Strategies

Optimize for different query patterns:

```yaml
schema:
  tables:
    # Time-series optimized partitioning
    user_events:
      type: "source"
      fields:
        # ... field definitions
      partitionBy: ["event_date", "event_type"]  # Multi-level partitioning
      clusterBy: ["user_id", "session_id"]       # Cluster by frequently filtered columns
    
    # Geography-based partitioning
    sales_data:
      type: "target"
      fields:
        # ... field definitions
      partitionBy: ["region", "quarter"]         # Business-aligned partitioning
      clusterBy: ["customer_tier", "product_category"]  # Analytics-optimized clustering
    
    # High-cardinality dimension partitioning
    product_analytics:
      type: "intermediate"
      fields:
        # ... field definitions
      partitionBy: ["analysis_date"]             # Time-based for data lifecycle
      clusterBy: ["category_id", "brand", "price_tier"]  # Multi-dimensional clustering
```

### Pattern 9: Incremental Processing Patterns

Implement efficient incremental data processing:

```yaml
transformations:
  incremental_user_features:
    type: "transform"
    description: "Incremental feature calculation with watermarking"
    source: ["user_events", "user_features_previous"]
    target: "user_features_current"
    sql: |
      -- Watermark-based incremental processing
      WITH max_processed_timestamp AS (
        SELECT COALESCE(MAX(last_updated), '1970-01-01'::timestamp) as watermark
        FROM user_features_previous
      ),
      
      new_events AS (
        SELECT *
        FROM user_events
        WHERE event_timestamp > (SELECT watermark FROM max_processed_timestamp)
          AND event_timestamp <= CURRENT_TIMESTAMP - INTERVAL 5 MINUTES  -- Late arrival buffer
      ),
      
      incremental_aggregates AS (
        SELECT 
          user_id,
          DATE(event_timestamp) as feature_date,
          COUNT(*) as new_event_count,
          COUNT(DISTINCT session_id) as new_session_count,
          SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as new_purchase_count
        FROM new_events
        GROUP BY user_id, DATE(event_timestamp)
      ),
      
      updated_features AS (
        SELECT 
          COALESCE(prev.user_id, inc.user_id) as user_id,
          COALESCE(prev.feature_date, inc.feature_date) as feature_date,
          COALESCE(prev.total_events, 0) + COALESCE(inc.new_event_count, 0) as total_events,
          COALESCE(prev.total_sessions, 0) + COALESCE(inc.new_session_count, 0) as total_sessions,
          COALESCE(prev.total_purchases, 0) + COALESCE(inc.new_purchase_count, 0) as total_purchases,
          CURRENT_TIMESTAMP as last_updated
        FROM user_features_previous prev
        FULL OUTER JOIN incremental_aggregates inc 
          ON prev.user_id = inc.user_id AND prev.feature_date = inc.feature_date
      )
      
      SELECT 
        user_id,
        feature_date,
        total_events,
        total_sessions,
        total_purchases,
        CASE WHEN total_events > 0 THEN total_purchases / total_events ELSE 0 END as conversion_rate,
        last_updated
      FROM updated_features
```

## Testing and Validation Patterns

### Pattern 10: Contract Testing Framework

Implement comprehensive testing for data contracts:

```yaml
# Test contract for validation
dataContractSpecification: "1.0.0"
id: "test-transaction-processing"
info:
  title: "Transaction Processing Test Contract"
  version: "1.0.0"
  description: "Test contract for validating transaction processing logic"

# Test data expectations
quality:
  expectations:
    # Schema validation tests
    - type: "custom"
      table: "processed_transactions"
      description: "Schema should match expected structure"
      expression: |
        CASE 
          WHEN (
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = 'processed_transactions'
          ) != 15  -- Expected column count
          THEN 0 
          ELSE 1 
        END
      threshold: 100.0
      severity: "error"
    
    # Transformation logic tests
    - type: "custom"
      table: "processed_transactions"
      description: "Currency conversion should be applied correctly"
      expression: |
        CASE 
          WHEN original_currency = 'USD' AND amount_usd != original_amount
          THEN 0
          WHEN original_currency = 'EUR' AND ABS(amount_usd - original_amount * 1.1) > 0.01
          THEN 0
          ELSE 1
        END
      threshold: 100.0
      severity: "error"
    
    # Data lineage tests
    - type: "custom"
      table: "processed_transactions"
      description: "All processed transactions should have source records"
      expression: |
        CASE 
          WHEN transaction_id NOT IN (SELECT transaction_id FROM raw_transactions)
          THEN 0 
          ELSE 1 
        END
      threshold: 100.0
      severity: "error"
    
    # Business rule tests
    - type: "custom"
      table: "processed_transactions"
      description: "High-value flag should be set correctly"
      expression: |
        CASE 
          WHEN amount_usd >= 500 AND is_high_value = false
          THEN 0
          WHEN amount_usd < 500 AND is_high_value = true
          THEN 0
          ELSE 1
        END
      threshold: 100.0
      severity: "error"
    
    # Performance tests
    - type: "custom"
      table: "processed_transactions"
      description: "Processing should complete within SLA"
      expression: |
        CASE 
          WHEN DATEDIFF(processing_timestamp, order_date) > 1
          THEN 0 
          ELSE 1 
        END
      threshold: 95.0
      severity: "warning"
```

These advanced patterns provide a comprehensive framework for implementing sophisticated data meshing scenarios while maintaining data quality, performance, and reliability. Each pattern can be adapted and combined to meet specific business requirements and technical constraints.
