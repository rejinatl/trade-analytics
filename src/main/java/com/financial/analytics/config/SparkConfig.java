package com.financial.analytics.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("Global Trade Order Analytics")
                .master("local[*]") // Use "local" for local execution, "local[*]" for cluster execution
                .getOrCreate();
    }
}
