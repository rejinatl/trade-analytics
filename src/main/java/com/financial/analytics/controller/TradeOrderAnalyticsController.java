package com.financial.analytics.controller;

import com.financial.analytics.model.response.CombinedResponseResource;
import com.financial.analytics.service.SparkMarketAbuseDetectionService;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TradeOrderAnalyticsController {

    @Getter
    private SparkMarketAbuseDetectionService sparkMarketAbuseDetectionService;

    @Autowired
    public void setSparkMarketAbuseDetectionService(SparkMarketAbuseDetectionService sparkMarketAbuseDetectionService) {
        this.sparkMarketAbuseDetectionService = sparkMarketAbuseDetectionService;
    }

    @GetMapping("/trades/metrics/{date}")
    public List<CombinedResponseResource> getDailyTradesCountPerInstrument(@PathVariable("date") String date) {

        return sparkMarketAbuseDetectionService.calculateDailyTradesPerInstrument(date);

    }
}
