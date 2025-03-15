package com.financial.analytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
public class TradeOrderResource {

    //private String[] tradedAccounts;
    private String tradeDate;
    private String instrument;
    private Long totalTrade;
    private Long tradeVolume;
    private String uniqueTradedAccount;
}
