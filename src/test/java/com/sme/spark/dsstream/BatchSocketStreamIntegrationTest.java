package com.sme.spark.dsstream;

import static java.util.Arrays.asList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.structuredstream.StructuredSocketStreamIntegrationTest;

import scala.Tuple2;

/**
 * Unit tests of {@link JavaDStream} stream.
 */
public class BatchSocketStreamIntegrationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StructuredSocketStreamIntegrationTest.class);
    private static final String HOST = "localhost";
    private static final int PORT = 9989;

    /**
     * <pre>
     * Test plan:
     * 1) run TerminalClient
     * 2) start "testDStreamSocketStream" unit test
     * 3) open TerminalClient window and type different phrases in line a few times as follow: test1 test2 test2 test3 test1 test1
     * 4) open JUnit window. Spark should calculate a count of words as follow:
     * (test2,2)
     * (test1,3)
     * (test3,1)
     * 5) type "exit" in TerminalClient window to stop client
     * </pre>
     */
    @Test
    void testDStreamSocketStream() throws Exception
    {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BatchSocketStream")
                .setMaster("local[2]");

        try (JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, new Duration(1000)))
        {
            JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream(HOST, PORT);

            JavaDStream<String> words = lines.flatMap(x -> asList(x.split(" ")).iterator());

            // Count each word in each batch
            JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

            // Print the first 5 elements of each RDD generated in this DStream to the console
            wordCounts.print(5);

            javaStreamingContext.start();
            javaStreamingContext.awaitTermination();
        }
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
         * @param args The list of arguments;
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

                    processLine(out, read);
                }
            }
        }

        private static void processLine(PrintWriter out, BufferedReader read) throws IOException
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
