package com.sme.spark.rdd;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.ASparkTest;

import scala.Tuple2;

/**
 * Unit tests of {@link JavaPairRDD} data.
 */
public class JavaPairRDDTest extends ASparkTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JavaPairRDDTest.class);

    @Test
    void testReduceByKeyWithPopularSort() throws Exception
    {
        JavaRDD<String> lines = javaSparkContext.parallelize(asList("one", "two", "one", "three", "four", "five", "four", "four")).cache();
        LOGGER.debug("Loaded {} lines", lines.collect());

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<Integer, String> counts = pairs
                .reduceByKey((a, b) ->
                {
                    LOGGER.debug("Reduce {} and {} keys", a, b);
                    return a + b;
                })
                .mapToPair(Tuple2::swap)
                .sortByKey(false);

        List<Tuple2<Integer, String>> collect = counts.collect();
        List<Tuple2<Integer, String>> expected = asList(new Tuple2<>(3, "four"), new Tuple2<>(2, "one"), new Tuple2<>(1, "two"), new Tuple2<>(1, "three"), new Tuple2<>(1, "five"));
        assertEquals(expected, collect);
    }

    @Test
    void testKeyBy() throws Exception
    {
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(asList(1, 2));

        List<Tuple2<String, Integer>> list = rdd.keyBy(i -> i.toString()).collect();
        assertEquals(new Tuple2<>("1", 1), list.get(0));
        assertEquals(new Tuple2<>("2", 2), list.get(1));
    }

    @Test
    void testFlatMapToPair() throws Exception
    {
        JavaRDD<String> lines = javaSparkContext.parallelize(asList("one 1 1 1", "two 2 2 2 2", "one 1")).cache();
        List<Tuple2<String, Integer>> list = lines
                .flatMap(line -> Arrays.stream(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((key1, key2) -> key1 + key2)
                .collect();

        assertEquals(asList(new Tuple2<>("two", 1), new Tuple2<>("1", 4), new Tuple2<>("one", 2), new Tuple2<>("2", 4)), list);
    }

    @Test
    @Disabled
    void testGroupByPair() throws Exception
    {
        JavaRDD<String> lines = javaSparkContext.parallelize(asList("one", "one", "four", "four", "one", "one", "four")).cache();
        LOGGER.debug("Loaded {} lines", lines.collect());

        JavaPairRDD<String, Iterator<Integer>> grouByKey = lines
                .mapToPair(word -> new Tuple2<>(word, 1))
                .groupByKey()
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._1, tuple2._2.iterator()));    // Iterator is not Serialized interface and cannot be used in spark RDD. Use Guava ImmutableList.of(iterator)

        List<Tuple2<String, Iterator<Integer>>> result = grouByKey.collect();

        //        Iterable<Integer> oneIterable = () -> Arrays.stream(new Integer[] {1, 1, 1, 1}).iterator();
        //        Iterable<Integer> fourIterable = () -> Arrays.stream(new Integer[] {1, 1, 1}).iterator();
        //        List<Tuple2<String, Iterable<Integer>>> expected = asList(new Tuple2<>("one", oneIterable), new Tuple2<>("four", fourIterable));

        List<Tuple2<String, Iterator<Integer>>> expected = asList(new Tuple2<>("one", asList(1, 1, 1, 1).iterator()), new Tuple2<>("four", asList(1, 1, 1).iterator()));

        assertEquals(expected, result);
    }

    @Test
    void testJoin()
    {
        JavaPairRDD<String, Integer> rdd1 = javaSparkContext.parallelize(asList(
                new Tuple2<>("one", 1),
                new Tuple2<>("two", 2),
                new Tuple2<>("two", 2),
                new Tuple2<>("two", 200),
                new Tuple2<>("three", 3)))
                .mapToPair(x -> new Tuple2<>(x._1, x._2))
                .reduceByKey((k1, k2) -> k1 + k2);

        JavaPairRDD<String, Integer> rdd2 = javaSparkContext.parallelize(asList(
                new Tuple2<>("one", 10),
                new Tuple2<>("two", 20),
                new Tuple2<>("three", 30)))
                .mapToPair(x -> new Tuple2<>(x._1, x._2));

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = rdd1.join(rdd2);
        List<Tuple2<String, Tuple2<Integer, Integer>>> list = join.collect();

        List<Tuple2<String, Tuple2<Integer, Integer>>> expected = asList(new Tuple2<>("two", new Tuple2<>(204, 20)),
                new Tuple2<>("one", new Tuple2<>(1, 10)),
                new Tuple2<>("three", new Tuple2<>(3, 30)));

        assertEquals(expected, list);
    }

    @Test
    void testFullOuterJoin()
    {
        JavaPairRDD<String, Integer> rdd1 = javaSparkContext.parallelize(asList(
                new Tuple2<>("one", 1),
                new Tuple2<>("two", 2),
                new Tuple2<>("two", 2),
                new Tuple2<>("two", 200),
                new Tuple2<>("three", 3)))
                .mapToPair(x -> new Tuple2<>(x._1, x._2))
                .reduceByKey((k1, k2) -> k1 + k2);

        JavaPairRDD<String, Integer> rdd2 = javaSparkContext.parallelize(asList(
                new Tuple2<>("one", 10),
                new Tuple2<>("two", 20),
                new Tuple2<>("three", 30)))
                .mapToPair(x -> new Tuple2<>(x._1, x._2));

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> fullOuterJoin = rdd1.fullOuterJoin(rdd2);
        List<Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>>> list = fullOuterJoin.collect();

        List<Tuple2<String, Tuple2<Optional<Integer>, Optional<Integer>>>> expected = asList(new Tuple2<>("two", new Tuple2<>(Optional.of(204), Optional.of(20))),
                new Tuple2<>("one", new Tuple2<>(Optional.of(1), Optional.of(10))),
                new Tuple2<>("three", new Tuple2<>(Optional.of(3), Optional.of(30))));

        assertEquals(expected, list);
    }

    @Test
    void testFoldReduce()
    {
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(asList(1, 1, 2, 3, 5, 8));
        Function2<Integer, Integer, Integer> add = (a, b) -> a + b;

        assertEquals(20, rdd.fold(0, add));
        assertEquals(20, rdd.reduce(add));
    }

    @Test
    void testFoldByKey()
    {
        List<Tuple2<Integer, Integer>> pairs = asList(
                new Tuple2<>(2, 1),
                new Tuple2<>(2, 1),
                new Tuple2<>(1, 1),
                new Tuple2<>(3, 2),
                new Tuple2<>(3, 4));
        JavaPairRDD<Integer, Integer> rdd = javaSparkContext.parallelizePairs(pairs);

        JavaPairRDD<Integer, Integer> sums = rdd.foldByKey(0, (a, b) -> a + b);

        assertEquals(1, sums.lookup(1).get(0).intValue());
        assertEquals(2, sums.lookup(2).get(0).intValue());
        assertEquals(6, sums.lookup(3).get(0).intValue());
    }

    @Test
    void testZip()
    {
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(asList(1, 2, 6));

        JavaDoubleRDD doubles = rdd.mapToDouble(x -> 1.0 * x);
        JavaPairRDD<Integer, Double> zipped = rdd.zip(doubles);

        List<Tuple2<Integer, Double>> list = zipped.collect();
        List<Tuple2<Integer, Double>> expected = asList(new Tuple2<>(1, 1.0), new Tuple2<>(2, 2.0), new Tuple2<>(6, 6.0));
        assertEquals(expected, list);
    }
}
