package com.financial.analytics.service;

import com.financial.analytics.config.DataSourceConnectionConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

@Service
@Slf4j
public class SparkCsvDataFileProcessorService implements SparkDataFileProcessorService {

    @Getter
    private SparkSession sparkSession;

    @Getter
    private DataSourceConnectionConfig dataSourceConnectionConfig;

    @Autowired
    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Autowired
    public void setDataSourceConnectionConfig(DataSourceConnectionConfig dataSourceConnectionConfig) {
        this.dataSourceConnectionConfig = dataSourceConnectionConfig;
    }

    @Value("${data.import.location}")
    private String basePath;

    @Value("${data.archive.location}")
    private String archiveLocation;

    @Override
    public void processDataFile() throws IOException {

        log.info("Processing and archiving CSV files...{}", basePath);
        try {
            List<String> csvFiles = extractAllDataFileFromPath(basePath);
            if(ObjectUtils.isNotEmpty(csvFiles)) {
                log.info("Saving data to DB...");
                Dataset<Row> data = sparkSession.read()
                        .option("header", true)
                        .option("inferSchema", "true")
                        .csv(csvFiles.toArray(new String[0]));

                // transform any mismatch in the table columns
                Dataset<Row> selectedData = getSelectedData(data);
                saveToDb(selectedData);
            }
            archiveProcessedFiles(basePath);

        } catch (Exception e) {
            log.error("Error processing data file: {}", e.getMessage());
        }
    }

    private void saveToDb(Dataset<Row> selectedData) {

        Properties connectionProperties = connectonProperties();
        selectedData = selectedData.na().fill( 0, new String[]{"display_qty", "last_qty"});
        selectedData.repartition(10)
                .write()
                .mode("append")
                .jdbc(dataSourceConnectionConfig.getUrl(), "trade_order_tracking", connectionProperties);
    }

    private List<String> extractAllDataFileFromPath(String basePath) {

        try (Stream<Path> files = Files.walk(Paths.get(basePath))) {
            return files
                    .filter(Files::isRegularFile)
                    .map(Path::toString)
                    .filter(string -> string.endsWith(".csv.gz"))
                    .toList();

        } catch (IOException e) {
            log.info("Cannot read form file path {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private void archiveProcessedFiles(String basePath) throws IOException {

        try (Stream<Path> paths = Files.list(Paths.get(basePath))) {

            paths.filter(Files::isDirectory).forEach(folder -> {
                try {

                    if (ObjectUtils.isNotEmpty(folder) && ObjectUtils.isNotEmpty(folder.getFileName())) {
                        Path archivePath = Paths.get(archiveLocation, String.valueOf(folder.getFileName()));
                        Files.createDirectories(archivePath.getParent());
                        Files.move(folder, archivePath, StandardCopyOption.REPLACE_EXISTING);
                        log.info("Archiving folder: {}", folder);
                    } else  {
                        log.info("Folder is empty: {}", folder);
                    }
                } catch (IOException e) {
                    log.error("Error in processing archive filename: {}", e.getMessage());
                }
            });
        }
    }

    private Dataset<Row> getSelectedData(Dataset<Row> data) {

        return data
                .withColumnRenamed("msgseqnum", "msg_seq_num")
                .withColumnRenamed("datetime", "msg_datetime");
    }

    private Properties connectonProperties() {

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", dataSourceConnectionConfig.getUsername());
        connectionProperties.put("password", dataSourceConnectionConfig.getPassword());
        connectionProperties.put("driver", dataSourceConnectionConfig.getDriverClassName());

        return connectionProperties;
    }
}
