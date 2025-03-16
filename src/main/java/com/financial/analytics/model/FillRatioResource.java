package com.financial.analytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FillRatioResource {

    private String tradeDate;
   // private String orderId;
    private String instrument;
    private String account;
    private Long displayQty;
    private Long totalOrderTraded;
    private String fillRatio;
    //private String messageType;


}
