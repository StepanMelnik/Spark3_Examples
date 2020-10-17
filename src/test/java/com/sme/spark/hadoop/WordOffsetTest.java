package com.sme.spark.hadoop;

import static com.sme.spark.hadoop.WordOffsetConstants.LINE;
import static com.sme.spark.hadoop.WordOffsetConstants.OFFSET;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test of line/chars offsets in a given file.
 */
class WordOffsetTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WordOffsetTest.class);
    private static final String NEW_LINE = System.getProperty("line.separator");
    private static Set<String> FILTER = new HashSet<>(asList("James", "John", "Robert", "Michael"));

    private static final String TEXT = "James and John in the first line" + NEW_LINE     // 32 + 1 chars
        + "John, Robert and Michael in the second line" + NEW_LINE                       // 43 + 1 chars
        + "James in the third line" + NEW_LINE                                           // 23 + 1 chars
        + "James, Oscar, Olga and Robert in the fourth line" + NEW_LINE                  // 48 + 1 chars
        + " and John,; Robert in the fourth line" + NEW_LINE;                            // 37 + 1 chars

    private SparkSession spark;
    private JavaSparkContext javaSparkContext;

    @BeforeEach
    public void setUp() throws Exception
    {
        spark = SparkSession
                .builder()
                .appName(this.getClass().getSimpleName())
                //.master("local[*]") // https://spark.apache.org/docs/latest/submitting-applications.html
                .master("local[4]") // Run Spark locally with as four worker threads
                .config("spark.sql.warehouse.dir", new File("SPARK_WAREHOUSE").getAbsolutePath())
                .enableHiveSupport()
                .getOrCreate();

        javaSparkContext = new JavaSparkContext(spark.sparkContext());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        spark.stop();
    }

    @Test
    void testFileOffsets() throws Exception
    {
        Path tempDir = Files.createTempDirectory(Paths.get("target"), "hadoopTemp");
        Path tempFile = Files.createTempFile(tempDir, "WordOffset", ".txt");

        Files.write(tempFile, TEXT.getBytes(StandardCharsets.UTF_8));

        JavaPairRDD<MapWritable, Text> data = javaSparkContext.newAPIHadoopFile(
                tempFile.toAbsolutePath().toString(),
                WordOffsetInputFormat.class,
                MapWritable.class,
                Text.class,
                new Configuration());

        JavaRDD<List<OffsetKey>> mapped = data.map(tuple ->
        {
            List<OffsetKey> list = new ArrayList<>();   // TODO use immutable list?

            MapWritable key = tuple._1();

            //Text fileName = (Text) key.get(new Text(FILE_NAME));
            Text textLine = (Text) key.get(new Text(LINE));
            Text textOffset = (Text) key.get(new Text(OFFSET));

            long charsOffset = Long.valueOf(textOffset.toString());
            long lineOffset = Long.valueOf(textLine.toString());

            String text = tuple._2().toString(); // extract text

            for (ImmutablePair<String, String> pair : TextSplitter.splitWithSpaces(text.toString()))
            {
                LOGGER.trace("Process \"{}\" word in the {} thread", pair.getLeft(), Thread.currentThread().getName());

                String space = pair.getRight();
                String wordWithNonWordChars = StringUtils.substringAfter(pair.getLeft(), space);
                String word = TextSplitter.splitAphaBetic(wordWithNonWordChars);

                if (FILTER.contains(word))
                {
                    long wordCharOffset = charsOffset + space.length();
                    list.add(new OffsetKey(word, lineOffset, wordCharOffset));
                }
                charsOffset = charsOffset + wordWithNonWordChars.length() + space.length();
            }

            return list;
        });

        List<OffsetKey> offsetKeys = mapped.flatMap(l -> l.iterator()).collect();
        LOGGER.debug("Created {} result", offsetKeys.toString());

        String expected = "[{\"key\":\"James\",\"lineOffset\":1,\"charOffset\":0}, "
            + "{\"key\":\"John\",\"lineOffset\":1,\"charOffset\":10}, "
            + "{\"key\":\"John\",\"lineOffset\":2,\"charOffset\":33}, "
            + "{\"key\":\"Robert\",\"lineOffset\":2,\"charOffset\":39}, "
            + "{\"key\":\"Michael\",\"lineOffset\":2,\"charOffset\":50}, "
            + "{\"key\":\"James\",\"lineOffset\":3,\"charOffset\":77}, "
            + "{\"key\":\"James\",\"lineOffset\":4,\"charOffset\":101}, "
            + "{\"key\":\"Robert\",\"lineOffset\":4,\"charOffset\":124}, "
            + "{\"key\":\"John\",\"lineOffset\":5,\"charOffset\":155}, "
            + "{\"key\":\"Robert\",\"lineOffset\":5,\"charOffset\":162}]";

        assertEquals(expected, offsetKeys.toString());

        Encoder<OffsetKey> offsetKeyEncoder = Encoders.bean(OffsetKey.class);

        spark.sql("CREATE TABLE IF NOT EXISTS offsetkeys (key STRING, lineOffset INT, charOffset INT) USING hive");

        Dataset<OffsetKey> dataSet = spark.createDataset(unmodifiableList(offsetKeys), offsetKeyEncoder);
        dataSet.show();

        dataSet.createOrReplaceTempView("offsetkeys");

        Dataset<Row> groupedByKeyList = spark.sql("SELECT key, COUNT(lineOffset), COUNT(charOffset) FROM offsetkeys GROUP BY 1").cache();
        groupedByKeyList.show();

        Dataset<Row> list = spark.sql("SELECT key, lineOffset, charOffset FROM offsetkeys ORDER BY key, lineOffset, charOffset").cache();
        list.show();

        /**
         * https://github.com/StepanMelnik/LargeFileParser projects returns the following result:
         * "{\"James\":[{\"lineOffset\":1,\"charOffset\":0},{\"lineOffset\":3,\"charOffset\":77},{\"lineOffset\":4,\"charOffset\":101}]," +
         * "\"Robert\":[{\"lineOffset\":2,\"charOffset\":39},{\"lineOffset\":4,\"charOffset\":124},{\"lineOffset\":5,\"charOffset\":162}]," +
         * "\"Michael\":[{\"lineOffset\":2,\"charOffset\":50}]," +
         * "\"John\":[{\"lineOffset\":1,\"charOffset\":10},{\"lineOffset\":2,\"charOffset\":33},{\"lineOffset\":5,\"charOffset\":155}]}");
         */

        List<Row> resultRows = list.collectAsList();

        List<Row> expectedRows = asList(RowFactory.create("James", 1, 0),
                RowFactory.create("James", 3, 77),
                RowFactory.create("James", 4, 101),
                RowFactory.create("John", 1, 10),
                RowFactory.create("John", 2, 33),
                RowFactory.create("John", 5, 155),
                RowFactory.create("Michael", 2, 50),
                RowFactory.create("Robert", 2, 39),
                RowFactory.create("Robert", 4, 124),
                RowFactory.create("Robert", 5, 162));
        assertEquals(expectedRows, resultRows);
    }
}
