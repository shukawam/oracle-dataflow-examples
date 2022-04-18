package me.shukawam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

/**
 * Example code for Spark Oracle DataSource.
 *
 * @author shukawam
 */
public class Main {
    public static void main(String... args) {
        SparkSession spark = SparkSession.builder().appName("spark-oracle-datasource").getOrCreate();
        Map<String, String> properties = getProperties(args);
        loadToAdw(spark, properties);
        spark.stop();
    }

    private static Map<String, String> getProperties(String... args) {
        Map<String, String> properties = new HashMap<>();
        properties.put("adbId", args[0]);
        properties.put("username", args[1]);
        properties.put("password", args[2]);
        properties.put("schema", args[3]);
        properties.put("fromTable", args[4]);
        properties.put("toTable", args[5]);
        return properties;
    }

    private static void loadToAdw(SparkSession spark, Map<String, String> properties) {
        Map<String, String> defaultOptions = new HashMap<>();
        defaultOptions.put("adbId", properties.get("adbId"));
        defaultOptions.put("user", properties.get("username"));
        defaultOptions.put("password", properties.get("password"));
        Dataset<Row> dataset = spark.read()
                .format("oracle")
                .options(defaultOptions)
                .option("dbtable", String.format("%s.%s", properties.get("schema"), properties.get("fromTable")))
                .load();
        dataset.show();
        dataset.write()
                .format("oracle")
                .options(defaultOptions)
                .option("dbtable", String.format("%s.%s", properties.get("schema"), properties.get("toTable")))
                .save();
    }

}
