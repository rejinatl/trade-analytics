package com.financial.analytics.controller;

import com.financial.analytics.model.response.CombinedResponseResource;
import com.financial.analytics.service.SparkMarketTradeOrderAnalyticService;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TradeOrderAnalyticsController {

    @Getter
    private SparkMarketTradeOrderAnalyticService sparkMarketTradeOrderAnalyticService;

    @Autowired
    public void setSparkMarketAbuseDetectionService(SparkMarketTradeOrderAnalyticService sparkMarketTradeOrderAnalyticService) {
        this.sparkMarketTradeOrderAnalyticService = sparkMarketTradeOrderAnalyticService;
    }

    @GetMapping("/trades-metrics/{date}")
    public List<CombinedResponseResource> getDailyTradesCountPerInstrument(@PathVariable("date") String date) {

        return sparkMarketTradeOrderAnalyticService.calculateDailyTradesPerInstrument(date);

    }
}
