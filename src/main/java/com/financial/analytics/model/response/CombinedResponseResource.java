package com.financial.analytics.model.response;

import com.financial.analytics.model.TopTradeOrderResource;
import com.financial.analytics.model.TradeOrderResource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CombinedResponseResource {

    private String tradeDate;
    private List<CombinedInstrumentData> tradeOrderResourceList;
    private List<TopTradeOrderResource> topTradedOrders;

}


