package com.sme.spark.rdd;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.ASparkTest;
import com.sme.spark.serialize.SerializableComparator;

/**
 * Unit tests of {@link JavaRDD} data operations.
 */
public class JavaRDDOperationsTest extends ASparkTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JavaRDDOperationsTest.class);

    @Test
    void testParallelize() throws Exception
    {
        List<Integer> list = IntStream.range(0, 100).boxed().collect(toList());
        JavaRDD<Integer> data = javaSparkContext.parallelize(list);

        //Integer max = data.max(Integer::compare);   // java.util.Comparator is not serializable
        Integer max = data.max(SerializableComparator.serialize(Integer::compare));
        assertEquals(99, max);
    }

    @Test
    public void testFilter() throws Exception
    {
        JavaRDD<String> lines = javaSparkContext.parallelize(asList("one", "two", "three", "four", "five")).cache();
        LOGGER.debug("Loaded {} lines", lines.collect());

        JavaRDD<String> filteredLines = lines.filter(s -> s.contains("o"));
        assertEquals(asList("one", "two", "four"), filteredLines.collect());
    }

    @Test
    public void testMapReduce() throws Exception
    {
        JavaRDD<String> lines = javaSparkContext.parallelize(asList("one", "two", "three", "four", "five")).cache();
        LOGGER.debug("Loaded {} lines", lines.collect());

        JavaRDD<Integer> mappedToLength = lines.map(s -> s.length());
        int totalLength = mappedToLength.reduce((i1, i2) -> i1 + i2);

        assertEquals(19, totalLength);
    }

    @Test
    public void testFlatMap() throws Exception
    {
        JavaRDD<String> lines = javaSparkContext.parallelize(asList("one two", "three four", "five six ten")).cache();
        LOGGER.debug("Loaded {} lines", lines.collect());

        JavaRDD<String> words = lines.flatMap(line -> Arrays.stream(line.split(" ")).iterator());
        assertEquals(asList("one", "two", "three", "four", "five", "six", "ten"), words.collect());
    }

    @Test
    public void testUnionTake() throws Exception
    {
        List<String> list = asList("This is a log line", "This is warn-1 log line", "This is warn-2 log line", "This is an error log line");

        JavaRDD<String> lines = javaSparkContext.parallelize(list);
        LOGGER.debug("Loaded {} lines", lines.collect());

        JavaRDD<String> warnLines = lines.filter(s -> s.contains("warn-"));
        JavaRDD<String> errorLines = lines.filter(s -> s.contains("error"));

        JavaRDD<String> badLines = warnLines.union(errorLines).cache();
        assertEquals(asList("This is warn-1 log line", "This is warn-2 log line", "This is an error log line"), badLines.collect());
        assertEquals(asList("This is warn-1 log line", "This is warn-2 log line"), badLines.take(2));
    }
}
