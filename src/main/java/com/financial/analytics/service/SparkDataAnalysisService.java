package com.financial.analytics.service;

import com.financial.analytics.model.response.CombinedResponseResource;

import java.util.List;

public interface SparkDataAnalysisService {

    List<CombinedResponseResource> calculateDailyTradesPerInstrument(String date);
}
