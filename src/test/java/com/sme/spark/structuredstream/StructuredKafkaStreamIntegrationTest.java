package com.sme.spark.structuredstream;

import static java.util.Arrays.asList;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.ASparkTest;

/**
 * Unit test to consume stream from Kafka.
 */
public class StructuredKafkaStreamIntegrationTest extends ASparkTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredKafkaStreamIntegrationTest.class);
    private static final String BOOTSTRAP_SERVERS = "192.168.0.109:9092";
    private static final String SUBSCRIBE_TYPE = "subscribe";
    private static final String TOPICS = "Topic1, Topic2";

    /**
     * <pre>
     * Test plan:
     * 1) start Kafka server as described in README.md and create Topic1 and Topic2 topics
     * 2) run "testConsumeKafkaWordCount" unit test
     * 3) run send a message by kafka to topic:
     *  sudo ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic Topic1 Topic2
     *  type: 111 222 111 222 222 333
     * 4) Spark job should print the following:
     * +-----+-----+
     * |value|count|
     * +-----+-----+
     * |  111|    2|
     * |  222|    3|
     * |  333|    1|
     * +-----+-----+
     * </pre>
     */
    @Test
    void testConsumeKafkaWordCount() throws Exception
    {
        Dataset<Row> stream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option(SUBSCRIBE_TYPE, TOPICS)
                .load();

        Dataset<Row> lines = stream.selectExpr("CAST(value AS STRING)");

        Dataset<String> words = lines.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x ->
                {
                    LOGGER.debug("Split {} line", x);
                    return asList(x.split(" ")).iterator();
                }, Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        query.awaitTermination();
    }
}
