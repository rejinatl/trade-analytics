package com.financial.analytics.controller;

import com.financial.analytics.service.SparkCsvDataFileProcessorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@Slf4j
public class TradeOrderIngestionController {

    @Getter
    private SparkCsvDataFileProcessorService sparkCsvDataFileProcessorService;

    @Autowired
    public void setSparkCsvDataFileProcessorService(SparkCsvDataFileProcessorService sparkCsvDataFileProcessorService) {
        this.sparkCsvDataFileProcessorService = sparkCsvDataFileProcessorService;
    }

    @GetMapping("/process-and-archive")
    public Map<String, String> processAndArchive() {
        try {

            sparkCsvDataFileProcessorService.processDataFile();

            return Map.of( "status", "success",
                            "message", "Processed, saved data to DB, and archived successfully.");
        } catch (Exception e) {

           log.error("Error during processing and archiving: {}", e.getMessage());
            return Map.of( "status", "failed",
                    "message", e.getMessage());
        }
    }
}
