package com.sme.spark.structuredstream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.ASparkTest;

import scala.Tuple2;

/**
 * Real time Socket stream unit test.
 */
public class StructuredSocketStreamIntegrationTest extends ASparkTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredSocketStreamIntegrationTest.class);
    private static final String HOST = "localhost";
    private static final int PORT = 9994;

    /**
     * <pre>
     * Test plan:
     * 1) run TerminalClient
     * 2) start "testSocketStreamToConsole" unit test
     * 3) open TerminalClient window and type the following: test1 test2 test2 test3 test1 test1
     * 4) open JUnit window. Spark should calculate a count of words as follow:
     * -------------------------------------------
     * Batch: 1
     * -------------------------------------------
     * +-----+-----+
     * |value|count|
     * +-----+-----+
     * |test3|    1|
     * |test1|    2|
     * |test2|    2|
     * +-----+-----+
     * 5) type the following in TerminalClient window: 111 333 222 111 222 111 222 111
     * 6) open JUnit window. Spark should calculate a count of words as follow:
     * -------------------------------------------
     * Batch: 2
     * -------------------------------------------
     * +-----+-----+
     * |value|count|
     * +-----+-----+
     * |test3|    1|
     * |test1|    2|
     * |  111|    4|
     * |test2|    2|
     * |  222|    3|
     * |  333|    1|
     * +-----+-----+
     * 7) type "exit" in TerminalClient window to stop client
     * </pre>
     */
    @Test
    void testSocketStreamToConsole() throws Exception
    {
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", HOST)
                .option("port", PORT)
                .load();

        Dataset<String> words = lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        // dataset should works with stream query and should be started
        //words.show();
        //LOGGER.debug("Get the following line against socket: " + words.collectAsList());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery wordCountsQuery = wordCounts
                .writeStream()
                .foreach(new ForeachWriter<Row>()
                {
                    @Override
                    public boolean open(long partitionId, long version)
                    {
                        LOGGER.debug("Open connection on {} partition with {} version", partitionId, version);
                        return true;
                    }

                    @Override
                    public void process(Row record)
                    {
                        LOGGER.debug("Processing {} record to connection", record);
                    }

                    @Override
                    public void close(Throwable errorOrNull)
                    {
                        LOGGER.error("Close the connection on error", errorOrNull);
                    }
                })
                .outputMode("complete")
                .format("console")
                .start();

        spark.streams().addListener(new StreamingQueryListener()
        {
            @Override
            public void onQueryProgress(QueryProgressEvent event)
            {
                LOGGER.debug("Query {} processing", event.progress().prettyJson());
            }

            @Override
            public void onQueryStarted(QueryStartedEvent event)
            {
                LOGGER.debug("Query {} started", event.id());
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event)
            {
                LOGGER.debug("Query {} terminated", event.id());
            }
        });

        wordCountsQuery.awaitTermination();
    }

    /**
     * <pre>
     * Test plan:
     * 1) run TerminalClient
     * 2) start "testSocketStreamWithWindowDuration" unit test
     * 3) open TerminalClient window and type the following: 1 11 111 ENTER 22 222 222 22 ENTER 333 333 33 33 ENTER 44 4444 44 4444 555 55, etc
     * 4) open JUnit window. Spark should calculate a count of words per tmestampas follow:
     * -------------------------------------------
     * Batch: 3
     * -------------------------------------------
     * +------------------------------------------+----+-----+
     * |window                                    |word|count|
     * +------------------------------------------+----+-----+
     * |[2020-08-10 11:07:00, 2020-08-10 11:07:10]|1   |1    |
     * |[2020-08-10 11:07:00, 2020-08-10 11:07:10]|111 |3    |
     * |[2020-08-10 11:07:00, 2020-08-10 11:07:10]|11  |1    |
     * |[2020-08-10 11:07:05, 2020-08-10 11:07:15]|1   |1    |
     * |[2020-08-10 11:07:05, 2020-08-10 11:07:15]|111 |3    |
     * |[2020-08-10 11:07:05, 2020-08-10 11:07:15]|11  |1    |
     * |[2020-08-10 11:07:15, 2020-08-10 11:07:25]|22  |1    |
     * |[2020-08-10 11:07:15, 2020-08-10 11:07:25]|222 |2    |
     * |[2020-08-10 11:07:20, 2020-08-10 11:07:30]|222 |2    |
     * |[2020-08-10 11:07:20, 2020-08-10 11:07:30]|33  |2    |
     * |[2020-08-10 11:07:20, 2020-08-10 11:07:30]|22  |1    |
     * |[2020-08-10 11:07:20, 2020-08-10 11:07:30]|333 |1    |
     * |[2020-08-10 11:07:25, 2020-08-10 11:07:35]|333 |1    |
     * |[2020-08-10 11:07:25, 2020-08-10 11:07:35]|33  |2    |
     * |[2020-08-10 11:07:35, 2020-08-10 11:07:45]|44  |2    |
     * |[2020-08-10 11:07:35, 2020-08-10 11:07:45]|4444|1    |
     * |[2020-08-10 11:07:40, 2020-08-10 11:07:50]|44  |2    |
     * |[2020-08-10 11:07:40, 2020-08-10 11:07:50]|4444|1    |
     * |[2020-08-10 11:07:45, 2020-08-10 11:07:55]|555 |2    |
     * |[2020-08-10 11:07:45, 2020-08-10 11:07:55]|55  |1    |
     * +------------------------------------------+----+-----+
     * 
     * 5) type "exit" in TerminalClient window to stop client
     * </pre>
     */
    @Test
    void testSocketStreamWithWindowDuration() throws Exception
    {
        final String windowDuration = "10 seconds";
        final String slideDuration = "5 seconds";

        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", HOST)
                .option("port", PORT)
                .option("includeTimestamp", true)
                .load();

        Dataset<Row> words = lines
                .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t ->
                {
                    List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                    LOGGER.debug("Split {} string to words with {} timestamp", t._1, t._2);

                    for (String word : t._1.split(" "))
                    {
                        LOGGER.debug("Create new Tuple2 instance with {} word and {} timestamp");
                        result.add(new Tuple2<>(word, t._2));
                    }
                    return result.iterator();
                }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
                .toDF("word", "timestamp");

        //LOGGER.debug("Created {} dataset", words.collectAsList());
        //words.show();

        Dataset<Row> windowedCounts = words.groupBy(
                functions.window(words.col("timestamp"), windowDuration, slideDuration),
                words.col("word")).count().orderBy("window");

        StreamingQuery query = windowedCounts.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .option("truncate", "false")
                .start();

        query.awaitTermination();
    }

    /**
     * Terminal client to connect to Spark stream by socket.
     */
    @SuppressWarnings("unused")
    private static class TerminalClient
    {
        /**
         * Main entry point to start terminal client.
         * 
         * @param args The list of arguments
         * @throws IOException Throw IO exception if occurs;
         * @throws InterruptedException Throw Interrupted exception if occurs.
         */
        public static void main(String[] args) throws IOException, InterruptedException
        {
            LOGGER.debug("Create new Socket");
            try (ServerSocket socket = new ServerSocket(PORT))
            {
                LOGGER.debug("Accept socket connection");
                Socket clientSocket = socket.accept();

                LOGGER.debug("Connection Received");
                OutputStream outputStream = clientSocket.getOutputStream();

                while (true)
                {
                    PrintWriter out = new PrintWriter(outputStream, true);
                    BufferedReader read = new BufferedReader(new InputStreamReader(System.in));

                    processLIne(out, read);
                }
            }
        }

        private static void processLIne(PrintWriter out, BufferedReader read) throws IOException
        {
            LOGGER.debug("Waiting for user to input some data");
            String data = read.readLine();

            exit(data);

            LOGGER.debug("Data received and now writing it to Socket");
            out.println(data);
        }

        private static void exit(String data)
        {
            if ("exit".equals(data))
            {
                System.exit(0);
            }
        }
    }
}
