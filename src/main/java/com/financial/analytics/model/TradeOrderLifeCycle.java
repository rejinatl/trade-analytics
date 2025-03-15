package com.financial.analytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TradeOrderLifeCycle {

    private String tradeDate;
    private String account;
    private String orderId;
    private String instrument;
    private Long totalOrderEntered;
    private Long totalOrderCancelled;
    private Long totalOrderTraded;

}
