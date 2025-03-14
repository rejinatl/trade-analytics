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
public class FillRatioResource {

    private String account;
    private Long totalFilledQty;
    private Long totalDisplayedQty;
    private Double fillRatio;
}
