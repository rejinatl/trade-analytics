package com.financial.analytics.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
@ConfigurationProperties(prefix = "spring.datasource")
public class DataSourceConnectionConfig {

    private String url;
    private String username;
    private String password;
    private String driverClassName;
}
