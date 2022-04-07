package me.shukawam.spark.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example code for ETL via Spark and SparkSQL.
 *
 * @author shukawam
 */
public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public Main() {
    }

    public static void main(String... args) {
        SparkSession spark = SparkSession.builder().appName("spark-etl-java").getOrCreate();
        etl(spark, args[0], args[1], args[2]);
        LOGGER.log(Level.INFO, "ETL was successful!");
        spark.stop();
    }

    private static void etl(SparkSession sparkSession, String airlinesPath, String flightPath, String output) {
        Dataset<Row> airlinesData = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(airlinesPath);
        LOGGER.log(Level.INFO, "airlines");
        airlinesData.show();
        Dataset<Row> flightData = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(flightPath);
        LOGGER.log(Level.INFO, "flights");
        flightData.show();
        Dataset<Row> airlinesDataOfficial = airlinesData.select(
                airlinesData.col("IATA_CODE"),
                airlinesData.col("AIRLINE").as("AIRLINE_NAME")
        );
        LOGGER.log(Level.INFO, "airlinesOfficial");
        airlinesDataOfficial.show();
        Dataset<Row> flightJoinedData = flightData.join(
                airlinesDataOfficial,
                flightData.col("AIRLINE").equalTo(airlinesDataOfficial.col("IATA_CODE")),
                "left"
        );
        LOGGER.log(Level.INFO, "flightJoined");
        flightJoinedData.show();
        Dataset<Row> avgDelay = flightJoinedData.groupBy(flightJoinedData.col("AIRLINE_NAME"))
                .agg(count(flightJoinedData.col("AIRLINE")).alias("FLIGHTS_COUNT"), avg(flightJoinedData.col("ARRIVAL_DELAY")).alias("AVG_ARRIVAL_DELAY"))
                .withColumn("AVG_ARRIVAL_DELAY_MINUTES", format_number(col("AVG_ARRIVAL_DELAY"), 2))
                .coalesce(1)
                .sortWithinPartitions(col("AVG_ARRIVAL_DELAY"))
                .drop("AVG_ARRIVAL_DELAY");
        LOGGER.log(Level.INFO, "avgDelay");
        avgDelay.show();
        avgDelay.write().json(output);
    }
}
