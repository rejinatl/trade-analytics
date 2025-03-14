package com.financial.analytics.service;

import com.financial.analytics.model.TradeOrderResource;

import java.util.List;

public interface SparkDataAnalysisService {

    List<TradeOrderResource> calculateDailyTradesPerInstrument();
}
