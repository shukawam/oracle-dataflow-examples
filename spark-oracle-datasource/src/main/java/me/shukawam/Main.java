package me.shukawam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.logging.Logger;

/**
 * Example code for Spark Oracle DataSource.
 *
 * @author shukawam
 */
public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String... args) {
        SparkSession spark = SparkSession.builder().appName("spark-oracle-datasource").getOrCreate();
        loadToAdw(spark, args[0], args[1], args[2]);
        spark.stop();
    }

    public static void loadToAdw(SparkSession spark, String salesChannelPath, String walletUri, String dbTable) {
        // data load from object storage
        Dataset<Row> salesChannel = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(salesChannelPath);
        LOGGER.info("SALES_CHANNEL");
        salesChannel.show();
        // data load to adw with wallet in object storage
        salesChannel.write()
                .format("oracle")
                .option("walletUri", walletUri)
                .option("dbtable", dbTable)
                .save();
    }
}
