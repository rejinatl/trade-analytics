package com.financial.analytics.service;

import com.financial.analytics.config.DataSourceConnectionConfig;
import com.financial.analytics.model.FillRatioResource;
import com.financial.analytics.model.TotalTradeOrderResource;
import com.financial.analytics.model.TradeOrderLifeCycle;
import com.financial.analytics.model.TradeOrderResource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.desc;

@Service
@Slf4j
public class SparkMarketAbuseDetectionService implements SparkDataAnalysisService {

    @Getter
    private SparkSession sparkSession;

    @Getter
    private DataSourceConnectionConfig dataSourceConnectionConfig;

    @Value("${spark.sql.session.timeZone:UTC}") // Defaults to UTC if not set
    private String sparkTimeZone;

    @Autowired
    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Autowired
    public void setDataSourceConnectionConfig(DataSourceConnectionConfig dataSourceConnectionConfig) {
        this.dataSourceConnectionConfig = dataSourceConnectionConfig;
    }

    @Override
    public List<TradeOrderResource> calculateDailyTradesPerInstrument() {

     try {

         sparkSession.conf().set("spark.sql.session.timeZone", sparkTimeZone);
         Dataset<Row> sparkSessionData = sparkSession
                 .read()
                 .format("jdbc")
                 .options(connectionProperties())
                 .option("partitionColumn", "id")
                 .option("lowerBound", 1)
                 .option("upperBound", 1000000)
                 .option("numPartitions", 10)
                 .option("fetchsize", "1000")
                 .load().cache();

         Dataset<Row> filterDataSet =  sparkSessionData.withColumn("tradeDate",
                         functions.to_date(sparkSessionData.col("msg_datetime"),
                                 sparkTimeZone))
                 .withColumn("accountId", functions.col("account"))
                 .filter(col("message_type").startsWith("TRADE_"))
                 .groupBy("tradeDate", "instrument")
                 .agg(
                         functions.count("*").alias("totalTradeCount"),
                         functions.sum("last_qty").alias("tradeVolume"),
                         functions.collect_set("accountId").alias("tradedAccounts"),
                         functions.countDistinct("account").alias("uniqueTradedAccountCount")

                 )
                 .orderBy("tradeDate","instrument");

         List<TradeOrderLifeCycle> orderLifeCycleDataSet = getOrderLifeCyclePerCalender(sparkSessionData);
         List<TotalTradeOrderResource> topTradedInstrumentData = getTopTradedInstrumentPerDay(sparkSessionData);
         List<FillRatioResource> fillRatioDataSet = getAccountFillRatio(sparkSessionData);


         return filterDataSet.as(Encoders.bean(TradeOrderResource.class)).collectAsList();

     } catch (Exception e) {

         log.error("error during market trading data analysis: {}", e.getMessage());
     }
        return Collections.emptyList();
    }

    public List<TradeOrderLifeCycle> getOrderLifeCyclePerCalender(Dataset<Row> sparkSessionData) {

        try {

            Dataset<Row> orderLifeCycleDataSet = sparkSessionData.withColumn("tradeDate",
                            functions.to_date(sparkSessionData.col("msg_datetime"), sparkTimeZone))
                    .filter(col("message_type").isin("ENTER", "CANCEL")
                            .or(col("message_type").rlike("^TRADE_")))
                    .groupBy("tradeDate", "instrument")
                    .agg(
                            // Count orders entered (message_type = ENTER)
                            functions.count(when(col("message_type").equalTo("ENTER"), 1)).alias("orderEntered"),
                            // Count orders cancelled (message_type = CANCEL)
                            functions.count(when(col("message_type").equalTo("CANCEL"), 1)).alias("orderCancelled"),
                            // Count trades (message_type = STARTS WITH TRADE_*)
                            functions.count(when(col("message_type").startsWith("TRADE_"), 1)).alias("orderTraded")

                    )
                    .orderBy("tradeDate", "instrument");

            List<TradeOrderLifeCycle> tradeOrderLifeCycles = orderLifeCycleDataSet.as(Encoders.bean(TradeOrderLifeCycle.class)).collectAsList();
            log.info("order life cycle data: {}", tradeOrderLifeCycles);

            return Optional.ofNullable(tradeOrderLifeCycles).filter(ObjectUtils::isNotEmpty)
                    .orElse(Collections.emptyList());

        } catch (Exception e) {
            log.error("error during life cycle data analysis: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private List<TotalTradeOrderResource> getTopTradedInstrumentPerDay(Dataset<Row> sparkSessionData) {

        try {

            Dataset<Row> topTradedInstrumentDataSet =
                    sparkSessionData.withColumn("price_last_qty", col("price").multiply(col("last_qty")))
                    .withColumn("tradeDate", functions.to_date(col("msg_datetime"), sparkTimeZone))
                    .filter(col("message_type").startsWith("TRADE_"))
                    .groupBy("tradeDate", "instrument")
                    .agg(
                            functions.sum("price_last_qty").alias("totalValue")
                    )
                    .withColumn("rank",
                            functions.row_number().over(Window.partitionBy("tradeDate").orderBy(functions.desc("totalValue"))))
                    .filter(col("rank").leq(5))  // Filter for top 5 instruments
                    .orderBy("tradeDate", "rank");

            List<TotalTradeOrderResource> topTradedInstrumentData = topTradedInstrumentDataSet.as(Encoders.bean(TotalTradeOrderResource.class)).collectAsList();
            log.info("order life cycle data: {}", topTradedInstrumentData);
            return Optional.ofNullable(topTradedInstrumentData).filter(ObjectUtils::isNotEmpty)
                    .orElse(Collections.emptyList());

        } catch (Exception e) {
            log.error("error during top traded instrument analysis: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    public List<FillRatioResource> getAccountFillRatio(Dataset<Row> sparkSessionData) {

        try {

            Dataset<Row> topFillRatioDataSet = sparkSessionData
                    .filter(col("message_type").startsWith("TRADE_"))
                    .groupBy("account")
                    .agg(
                            sum("last_qty").alias("totalFilledQty"),
                            sum("display_qty").alias("totalDisplayedQty")
                    )
                    .withColumn(
                            "fillRatio",
                            when(col("totalDisplayedQty").notEqual(0),
                                    col("totalFilledQty").divide(col("totalDisplayedQty")))
                                    .otherwise(lit(0))
                    )
                    .orderBy(desc("fillRatio"));

            List<FillRatioResource> totalFillRatioData = topFillRatioDataSet.as(Encoders.bean(FillRatioResource.class)).collectAsList();
            log.info("account fill ratio: {}", totalFillRatioData);
            return Optional.ofNullable(totalFillRatioData).filter(ObjectUtils::isNotEmpty)
                    .orElse(Collections.emptyList());

        } catch (Exception e) {
            log.error("error during account fill ratio analysis: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private Map<String, String> connectionProperties() {

        Map<String, String> options = new HashMap<>();
        options.put("url", dataSourceConnectionConfig.getUrl());
        options.put("user", dataSourceConnectionConfig.getUsername());
        options.put("password", dataSourceConnectionConfig.getPassword());
        options.put("driver", dataSourceConnectionConfig.getDriverClassName());
        options.put("dbtable", "trade_order_tracking");

        return options;
    }
}
