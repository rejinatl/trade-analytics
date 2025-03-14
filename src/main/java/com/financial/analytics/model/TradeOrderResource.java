package com.financial.analytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TradeOrderResource {

    private String[] tradedAccounts;
    private String tradeDate;
    private String instrument;
    private Long totalTradeCount;
    private Long tradeVolume;
    private String uniqueTradedAccountCount;

}
