package com.financial.analytics.controller;

import com.financial.analytics.model.TradeOrderResource;
import com.financial.analytics.service.SparkMarketAbuseDetectionService;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class TradeAnalysisController {

    @Getter
    private SparkMarketAbuseDetectionService sparkMarketAbuseDetectionService;

    @Autowired
    public void setSparkMarketAbuseDetectionService(SparkMarketAbuseDetectionService sparkMarketAbuseDetectionService) {
        this.sparkMarketAbuseDetectionService = sparkMarketAbuseDetectionService;
    }

    @GetMapping("/trades/daily-count-per-instrument")
    public Map<String, Map<String, List<TradeOrderResource>>> getDailyTradesCountPerInstrument() {

        List<TradeOrderResource> resources =  sparkMarketAbuseDetectionService.calculateDailyTradesPerInstrument();
        Map<String, List<TradeOrderResource>> ff1 = resources.stream()
                .collect(Collectors.groupingBy(TradeOrderResource::getInstrument));


        Map<String, Map<String, List<TradeOrderResource>>> groupedTrades = resources.stream()
                .collect(Collectors.groupingBy(TradeOrderResource::getTradeDate,
                        Collectors.groupingBy(TradeOrderResource::getInstrument)));

        return groupedTrades;

    }
}
