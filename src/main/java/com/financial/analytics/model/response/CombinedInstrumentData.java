package com.financial.analytics.model.response;

import com.financial.analytics.model.FillRatioResource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CombinedInstrumentData {

    private String instrument;
    private Long totalTradedCount;
    private Long totalTradedVolume;
    private String uniqueTradedAccount;
    private Long totalOrderEntered;
    private Long totalOrderCancelled;
    private List<FillRatioResource> fillRatios;
}
