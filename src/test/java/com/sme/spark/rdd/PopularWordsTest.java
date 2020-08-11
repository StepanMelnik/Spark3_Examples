package com.sme.spark.rdd;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import com.sme.spark.ASparkTest;

import scala.Tuple2;

/**
 * A simple test to fetch top popular words.
 */
public class PopularWordsTest extends ASparkTest
{
    @Test
    void testPopularWords() throws Exception
    {
        JavaRDD<String> lines = javaSparkContext.parallelize(Arrays.asList("one two three four five", "three two one, five", "five four", "five one"));
        List<String> result = topX(lines, 3);
        assertEquals(Arrays.asList("five", "two", "one"), result);
    }

    private List<String> topX(JavaRDD<String> lines, int x)
    {
        return lines.flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((i1, i2) -> i1 + i2)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                //.map(Tuple2::_2) Serialization error
                .map(t -> t._2())
                .take(x);
    }
}
