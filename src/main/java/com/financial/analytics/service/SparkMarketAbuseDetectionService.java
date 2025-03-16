package com.financial.analytics.service;

import com.financial.analytics.config.DataSourceConnectionConfig;
import com.financial.analytics.model.FillRatioResource;
import com.financial.analytics.model.TopTradeOrderResource;
import com.financial.analytics.model.TradeOrderLifeCycle;
import com.financial.analytics.model.TradeOrderResource;
import com.financial.analytics.model.response.CombinedInstrumentData;
import com.financial.analytics.model.response.CombinedResponseResource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.format_number;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.count;

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
    public  List<CombinedResponseResource> calculateDailyTradesPerInstrument(String date) {

         try {

             sparkSession.conf().set("spark.sql.session.timeZone", sparkTimeZone);
             String query = String.format("(SELECT id, msg_seq_num, order_id, instrument, account," +
                     "side, price, display_qty, last_qty, message_type, msg_datetime FROM trade_order_tracking " +
                     "WHERE DATE(msg_datetime) = '%s' ) AS filtered_data", date);

             Dataset<Row> sparkSessionData = sparkSession
                     .read()
                     .jdbc(dataSourceConnectionConfig.getUrl(), query, connectonProperties())
                     .cache();

             log.info("spark session here");

             List<TradeOrderResource> tradeOrderResourceList = getTradeOrderGroupByInstrument(sparkSessionData);
             List<TopTradeOrderResource> topTradedInstrumentData = getTopTradedInstrumentPerDay(sparkSessionData);
             List<FillRatioResource> fillRatioDataSet = getAccountFillRatio(sparkSessionData);

             return combinedResult(tradeOrderResourceList,topTradedInstrumentData, fillRatioDataSet);

         } catch (Exception e) {
             log.error("error during market trading data analysis: {}", e.getMessage());
             return Collections.emptyList();
         }
    }

    private List<TradeOrderResource> getTradeOrderGroupByInstrument(Dataset<Row> sparkSessionData) {

        Dataset<Row> filterDataSet =  sparkSessionData.withColumn("tradeDate", functions.to_date(sparkSessionData.col("msg_datetime"), sparkTimeZone))

                .groupBy("tradeDate", "instrument")
                .agg(

                        coalesce(count(when(col("message_type").startsWith("TRADE_"), col("last_qty"))),lit(0)).alias("totalTradedCount"),

                        coalesce(sum(when(col("message_type").equalTo("ENTER"), col("display_qty"))),lit(0)).alias("totalOrderEntered"),

                        coalesce(sum(when(col("message_type").startsWith("TRADE_"), col("last_qty"))),lit(0)).alias("totalTradedVolume"),

                        coalesce(functions.countDistinct(when(col("message_type").startsWith("TRADE_"), col("account"))),lit(0)).alias("uniqueTradedAccount"),

                        coalesce(sum(when(col("message_type").equalTo("CANCEL"), col("display_qty"))),lit(0)).alias("totalOrderCancelled")

                )
                .orderBy("tradeDate","instrument");

        List<TradeOrderResource> tradeOrderResourceList = filterDataSet.as(Encoders.bean(TradeOrderResource.class)).collectAsList();

        return Optional.ofNullable(tradeOrderResourceList).filter(ObjectUtils::isNotEmpty)
                .orElse(Collections.emptyList());

    }

    private List<TopTradeOrderResource> getTopTradedInstrumentPerDay(Dataset<Row> sparkSessionData) {

        try {

            Dataset<Row> topTradedInstrumentDataSet =
                    sparkSessionData.withColumn("price_last_qty", col("price").multiply(col("last_qty")))
                    .withColumn("tradeDate", functions.to_date(col("msg_datetime"), sparkTimeZone))
                    //.filter(col("message_type").startsWith("TRADE_"))
                    .groupBy("tradeDate", "instrument")
                    .agg(
                            sum("price_last_qty").alias("totalTradeAmount")
                    )
                    .withColumn("rank",
                            functions.row_number().over(Window.partitionBy("tradeDate").orderBy(functions.desc("totalTradeAmount"))))
                    .filter(col("rank").leq(5))  // filter for top 5 instruments
                    .orderBy("tradeDate", "rank");

            List<TopTradeOrderResource> topTradedInstrumentData = topTradedInstrumentDataSet.as(Encoders.bean(TopTradeOrderResource.class)).collectAsList();
            log.info("top traded instrument data: {}", topTradedInstrumentData);
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
                    .withColumn("tradeDate", functions.to_date(col("msg_datetime"), sparkTimeZone))
                    .withColumn("instrument", col("instrument"))
                    .withColumn("account", col("account"))
                    .groupBy("tradeDate","instrument","account")
                    .agg(
                            coalesce(sum(when(col("message_type").equalTo("ENTER"), col("display_qty"))),lit(0)).alias("displayQty"),
                            coalesce(sum(when(col("message_type").startsWith("TRADE_"), col("last_qty"))),lit(0)).alias("totalOrderTraded")

                    )
                    .withColumn(
                            "fillRatioUnformatted",
                            when(col("displayQty").notEqual(0),
                                    col("totalOrderTraded").divide(col("displayQty")).multiply(100))
                                    .otherwise(lit(0)))
                    .withColumn("fillRatio",
                            when(col("fillRatioUnformatted").mod(1).equalTo(0),
                                    col("fillRatioUnformatted").cast("int").cast("string"))
                                    .otherwise(format_number(col("fillRatioUnformatted"), 2)))
                    .filter(col("fillRatio").notEqual("0"))
                    .orderBy(desc("fillRatio"));

            List<FillRatioResource> totalFillRatioData = topFillRatioDataSet.as(Encoders.bean(FillRatioResource.class)).collectAsList();

            return Optional.ofNullable(totalFillRatioData).filter(ObjectUtils::isNotEmpty)
                    .orElse(Collections.emptyList());

        } catch (Exception e) {
            log.error("error during account fill ratio analysis: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private List<CombinedResponseResource> combinedResult(List<TradeOrderResource> tradeOrderResourceList,
                                                   List<TopTradeOrderResource> topTradedInstrumentData,
                                                   List<FillRatioResource> fillRatioDataSet) {

        CombinedResponseResource tradeOrderResponseResource = new CombinedResponseResource();

        // Group top traded orders by tradeDate and instrument
        Map<String, List<TopTradeOrderResource>> topTradedMap = topTradedInstrumentData.stream()
                .collect(Collectors.groupingBy(TopTradeOrderResource::getTradeDate));

        // Group fill ratios by tradeDate and instrument
        Map<String, List<FillRatioResource>> fillRatioMap = fillRatioDataSet.stream()
                .collect(Collectors.groupingBy(data -> data.getTradeDate() + "_" + data.getInstrument()));

       return tradeOrderResourceList.stream()
                .collect(Collectors.groupingBy(TradeOrderResource::getTradeDate))
                .entrySet().stream()
                .map(entry -> {

                    String tradeDate = entry.getKey();
                    List<CombinedInstrumentData> instruments = entry.getValue().stream().map(order -> {
                        String key = order.getTradeDate() + "_" + order.getInstrument();
                        List<FillRatioResource> ratios = fillRatioMap.getOrDefault(key, Collections.emptyList());

                        CombinedInstrumentData combined = new CombinedInstrumentData();
                        combined.setInstrument(order.getInstrument());
                        combined.setTotalTradedCount(order.getTotalTradedCount());
                        combined.setTotalTradedVolume(order.getTotalTradedVolume());
                        combined.setUniqueTradedAccount(order.getUniqueTradedAccount());
                        combined.setTotalOrderEntered(order.getTotalOrderEntered());
                        combined.setTotalOrderCancelled(order.getTotalOrderCancelled());
                        combined.setFillRatios(ratios);

                        return combined;
                    }).toList();

                    List<TopTradeOrderResource> topTrades = topTradedMap.getOrDefault(tradeDate, Collections.emptyList());

                    tradeOrderResponseResource.setTradeDate(tradeDate);
                    tradeOrderResponseResource.setTradeOrderResourceList(instruments);
                    tradeOrderResponseResource.setTopTradedOrders(topTrades);

                    return tradeOrderResponseResource;

                }).toList();
    }

    private List<TradeOrderLifeCycle> getOrderLifeCyclePerCalender(Dataset<Row> sparkSessionData) {

        try {

            Dataset<Row> orderLifeCycleDataSet = sparkSessionData.withColumn("tradeDate",
                            functions.to_date(sparkSessionData.col("msg_datetime"), sparkTimeZone))
                    .withColumn("orderId", functions.col("order_id"))
                    .withColumn("instrument", functions.col("instrument"))
                    .groupBy("orderId", "account", "tradeDate","instrument")
                    .agg(
                            // Sum of orders entered (message_type = ENTER)
                            coalesce(sum(when(col("message_type").equalTo("ENTER"), col("display_qty"))),lit(0)).alias("totalOrderEntered"),
                            // Sum of orders traded (message_type = TRADE_*)
                            coalesce(sum(when(col("message_type").startsWith("TRADE_"), col("last_qty"))),lit(0)).alias("totalOrderTraded"),
                            // Sum of orders cancelled (message_type = CANCEL)
                            coalesce(sum(when(col("message_type").equalTo("CANCEL"), col("display_qty"))),lit(0)).alias("totalOrderCancelled")
                    )
                    .orderBy("tradeDate", "orderId");

            List<TradeOrderLifeCycle> tradeOrderLifeCycles = orderLifeCycleDataSet.as(Encoders.bean(TradeOrderLifeCycle.class)).collectAsList();

            return Optional.ofNullable(tradeOrderLifeCycles).filter(ObjectUtils::isNotEmpty)
                    .orElse(Collections.emptyList());

        } catch (Exception e) {
            log.error("error during life cycle data analysis: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private Properties connectonProperties() {

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dataSourceConnectionConfig.getUsername());
        connectionProperties.put("password", dataSourceConnectionConfig.getPassword());
        connectionProperties.put("driver", dataSourceConnectionConfig.getDriverClassName());

        return connectionProperties;
    }
}
