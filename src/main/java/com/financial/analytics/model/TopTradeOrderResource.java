package com.financial.analytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TopTradeOrderResource {

    private String tradeDate;
    private String instrument;
    private BigDecimal totalValue;
    private Long rank;

}
