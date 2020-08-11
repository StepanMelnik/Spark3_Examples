package com.sme.spark.loadsave;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.ASparkTest;

import scala.Tuple2;

/**
 * Provides unit tests to load and save Text data.
 */
public class LoadSaveTextDataTest extends ASparkTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadSaveTextDataTest.class);

    private static final String EMPLOYEESFILE = "employees_per_line.txt";
    private static final Pattern SPACE = Pattern.compile(", ");

    @Test
    public void testLoadTestFile() throws Exception
    {
        Dataset<String> dataSet = spark.read().textFile(getPath(EMPLOYEESFILE));
        LOGGER.debug("{} columns", Arrays.toString(dataSet.columns()));

        JavaRDD<String> input = dataSet.javaRDD().cache();
        LOGGER.debug("Fetched {} employees", input.collect());

        assertEquals(12, input.count());
        List<String> filteredRows = input
                .filter(l -> l.contains("LastName10") || l.contains("LastName11") || l.contains("LastName12"))
                .take(2);
        assertEquals(asList("10, FirstName10, LastName10, 2020-08-10, Tester, 4000.0", "11, FirstName11, LastName11, 2020-08-11, Tester, 4100.0"), filteredRows);

        JavaRDD<String> words = input.flatMap(s -> asList(SPACE.split(s)).iterator());
        LOGGER.debug("Fetched {} words", words.collect());

        JavaPairRDD<String, Integer> pairWords = input.mapToPair(s ->
        {
            return new Tuple2<>(s, 1);
        });

        pairWords.partitionBy(new HashPartitioner(10));
        LOGGER.debug("Used {} partitions", words.getNumPartitions());

        List<Tuple2<String, Integer>> pairsList = pairWords.collect();
        List<Tuple2<String, Integer>> expectedList = asList(new Tuple2<>("1, FirstName1, LastName1, 2020-08-01, Tester, 3100.0", 1),
                new Tuple2<>("2, FirstName2, LastName2, 2020-08-02, Tester, 3200.0", 1));
        assertEquals(expectedList, pairsList.stream().limit(2).collect(toList()));
    }

    @Test
    public void testLoadMultiFiles() throws Exception
    {
        String parentPath = getParentPath(EMPLOYEESFILE);

        Dataset<String> dataSet = spark.read().textFile(parentPath + "/dir1/employees_per_line_*.txt");
        assertEquals(12, dataSet.count());
    }
}
