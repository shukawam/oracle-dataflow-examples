package example;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType$;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class StructuredKafkaWordCount {

    public static void main(String[] args) throws Exception {
        Logger log = LogManager.getLogger(StructuredKafkaWordCount.class);
        log.info("Started StructuredKafkaWordCount");
        Thread.setDefaultUncaughtExceptionHandler((thread, e) -> {
            log.error("Exception uncaught: ", e);
        });
        Map<String, String> params = parseArgs(args);
        SparkSession spark;
        SparkConf conf = new SparkConf();
        if (conf.contains("spark.master")) {
            spark = createDataFLowSession();
        } else {
            spark = createLocalSession();
        }
        DataStreamReader dataStreamReader = createDataStreamReader(spark, params);
        // Create DataFrame representing the stream of input lines from Kafka
        Dataset<Row> lines = dataStreamReader
                .load()
                .selectExpr("CAST(value AS STRING)");
        // Split the lines into timestamp and words
        StructType wordsSchema = StructType$.MODULE$.apply(
                new StructField[]{
                        StructField.apply("timestamp", TimestampType$.MODULE$, true, Metadata.empty()),
                        StructField.apply("value", StringType$.MODULE$, true, Metadata.empty())
                }
        );
        ExpressionEncoder<Row> encoder = RowEncoder.apply(wordsSchema);
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Dataset<Row> words = lines
                .flatMap(
                        (FlatMapFunction<Row, Row>) row -> {
                            // parse Kafka record in format: "timestamp(iso8601) text"
                            String text = row.getString(0);
                            String timestampString = text.substring(0, 25);
                            String message = text.substring(26);
                            Timestamp timestamp = new Timestamp(dateFormat.parse(timestampString).getTime());
                            return Arrays.asList(message.split(" ")).stream()
                                    .map(word -> RowFactory.create(timestamp, word)).iterator();
                        }, encoder);
        // Time window aggregation
        Dataset<Row> wordCounts = words
                .withWatermark("timestamp", "1 minutes")
                .groupBy(
                        functions.window(functions.col("timestamp"), "1 minutes", "1 minutes"),
                        functions.col("value")
                )
                .count()
                .selectExpr("CAST(window.start AS timestamp) AS START_TIME",
                        "CAST(window.end AS timestamp) AS END_TIME",
                        "value AS WORD",
                        "CAST(count AS long) AS COUNT");
        wordCounts.printSchema();
        // Reducing to a single partition
        wordCounts = wordCounts.coalesce(1);
        // Start streaming query
        StreamingQuery query = null;
        switch (params.get("type")) {
            case "console":
                query = outputToConsole(wordCounts, params.get("checkpointLocation"));
                break;
            case "csv":
                query = outputToCsv(wordCounts, params.get("checkpointLocation"), params.get("outputLocation"));
                break;
            default:
                System.err.println("Unknown type " + params.get("type"));
                System.exit(1);
        }
        query.awaitTermination();
    }

    private static void printUsage() {
        System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
                "<subscribe-topics> <kafkaAuthentication> <checkpoint-location> <type> ...");
        System.err.println("<kafkaAuthentication>: plain <username> <password>");
        System.err.println("<kafkaAuthentication>: RP <stream-pool-id>");
        System.err.println("<type>: console");
        System.err.println("<type>: csv <output-location>");
        System.exit(1);
    }

    private static Map<String, String> parseArgs(String... args) {
        Map<String, String> params = new HashMap<>();
        if (args.length < 4) {
            printUsage();
        }
        int parsedArgs = 0;
        String bootstrapServers = args[0];
        params.put("bootstrapServers", bootstrapServers);
        String topics = args[1];
        params.put("topics", topics);
        String kafkaAuthentication = args[2];
        params.put("kafkaAuthentication", kafkaAuthentication);
        String kafkaUsername = null;
        String kafkaPassword = null;
        String kafkaStreamPoolId = null;
        switch (kafkaAuthentication) {
            case "plain":
                if (args.length < 5) {
                    printUsage();
                }
                kafkaUsername = args[3];
                params.put("kafkaUsername", kafkaUsername);
                kafkaPassword = args[4];
                params.put("kafkaPassword", kafkaPassword);
                parsedArgs = 4;
                System.err.println(
                        "Using PlainLoginModule for Kafka authentication, username = " + kafkaUsername + " userpassword = "
                                + kafkaPassword);
                break;
            case "RP":
                if (args.length < 4) {
                    printUsage();
                }
                kafkaStreamPoolId = args[3];
                params.put("kafkaStreamPoolId", kafkaStreamPoolId);
                parsedArgs = 3;
                System.err.println("Using ResourcePrincipalsLoginModule for Kafka authentication, OSS Kafka Stream Pool Id = "
                        + kafkaStreamPoolId);
                break;
            default:
                printUsage();
        }
        String checkpointLocation = args[parsedArgs + 1];
        params.put("checkpointLocation", checkpointLocation);
        String type = args[parsedArgs + 2];
        params.put("type", type);
        String outputLocation = null;
        switch (type) {
            case "console":
                System.err.println("Using console output sink");
                break;
            case "csv":
                if (args.length < parsedArgs + 3) {
                    printUsage();
                }
                outputLocation = args[parsedArgs + 3];
                params.put("outputLocation", outputLocation);
                System.err.println("Using csv output sink, output location = " + outputLocation);
                break;
            default:
                printUsage();
        }
        return params;
    }

    private static SparkSession createDataFLowSession() {
        return SparkSession.builder()
                .appName("StructuredKafkaWordCount")
                .config("spark.sql.streaming.minBatchesToRetain", "10")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "300")
                .getOrCreate();
    }

    private static SparkSession createLocalSession() {
        return SparkSession.builder()
                .appName("StructuredKafkaWordCount")
                .master("local[*]")
                .config("spark.sql.streaming.minBatchesToRetain", "10")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "300")
                .getOrCreate();
    }

    private static DataStreamReader createDataStreamReader(SparkSession spark, Map<String, String> params) {
        DataStreamReader dataStreamReader = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", params.get("bootstrapServers"))
                .option("subscribe", params.get("topics"))
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.max.partition.fetch.bytes", 1024 * 1024) // limit request size to 1MB per partition
                .option("startingOffsets", "latest");
        switch (params.get("kafkaAuthentication")) {
            case "plain":
                dataStreamReader
                        .option("kafka.sasl.mechanism", "PLAIN")
                        .option("kafka.sasl.jaas.config",
                                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                                        + params.get("kafkaUsername") + "\" password=\"" + params.get("kafkaPassword") + "\";");
                break;
            case "RP":
                dataStreamReader
                        .option("kafka.sasl.mechanism", "OCI-RSA-SHA256")
                        .option("kafka.sasl.jaas.config",
                                "com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule required intent=\"streamPoolId:"
                                        + params.get("streamPoolId"));
                break;
            default:
                System.exit(1);
        }
        return dataStreamReader;
    }

    private static StreamingQuery outputToConsole(Dataset<Row> wordCounts, String checkpointLocation)
            throws TimeoutException {
        return wordCounts
                .writeStream()
                .format("console")
                .outputMode("complete")
                .option("checkpointLocation", checkpointLocation)
                .start();
    }

    private static StreamingQuery outputToCsv(Dataset<Row> wordCounts, String checkpointLocation,
                                              String outputLocation) throws TimeoutException {
        return wordCounts
                .writeStream()
                .format("csv")
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation)
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .option("path", outputLocation)
                .start();
    }
}
