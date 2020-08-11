package com.sme.spark.dsstream;

import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/**
 * Unit test to consume batch stream from Kafka.
 */
public class DirectBatchKafkaStreamIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectBatchKafkaStreamIntegrationTest.class);

    private static final String BROKERS = "192.168.0.109:9092";
    private static final String GROUP_ID = "consumer-group";
    private static final String TOPICS = "Topic1,Topic2";
    private static final Pattern SPACE = Pattern.compile(" ");

    /**
     * <pre>
     * Test plan:
     * 1) start Kafka server as described in README.md and create Topic1 and Topic2 topics
     * 2) run "testConsumeKafkaWordCount" unit test
     * 3) run send a message by kafka to topic:
     *  sudo ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic Topic1 Topic2
     *  type: 111 222 111 222 222 333
     * 4) Spark job should print the following:
     * (222,3)
     * (111,2)
     * (333,1)
     * </pre>
     */
    @Test
    void testConsumeKafkaWordCount() throws Exception
    {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BatchSocketStream")
                .setMaster("local[2]");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Set<String> topicsSet = new HashSet<>(asList(TOPICS.split(",")));

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        JavaDStream<String> lines = messages.map((Function<ConsumerRecord<String, String>, String>) f ->
        {
            LOGGER.debug("Process {} record", f);
            return f.value();
        });
        JavaDStream<String> words = lines.flatMap(x -> asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordCounts = words
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
