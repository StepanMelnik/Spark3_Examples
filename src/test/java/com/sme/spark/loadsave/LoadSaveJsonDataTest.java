package com.sme.spark.loadsave;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.ASparkTest;
import com.sme.spark.model.Employee;
import com.sme.spark.util.ObjectMapperUtil;
import com.sme.spark.util.PojoGenericBuilder;

/**
 * Provides unit tests to load and save JSON data.
 */
public class LoadSaveJsonDataTest extends ASparkTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadSaveJsonDataTest.class);
    private static final String EMPLOYEESFILE = "employees_per_line.json";

    @Test
    public void testLoadTestFile() throws Exception
    {
        Dataset<String> dataSet = spark.read().textFile(getPath(EMPLOYEESFILE)).cache();
        LOGGER.debug("{} columns", Arrays.toString(dataSet.columns()));

        JavaRDD<String> input = dataSet.javaRDD();

        JavaRDD<Employee> result = input.mapPartitions(new ParseJson());
        LOGGER.debug("Fetched {} employees", result.collect());

        List<Employee> expected = asList(
                new PojoGenericBuilder<>(Employee::new)
                        .with(Employee::setId, 1L)
                        .with(Employee::setFirstName, "FirstName 1")
                        .with(Employee::setLastName, "LastName 1")
                        .with(Employee::setRole, "Tester")
                        .with(Employee::setSalary, 3100.0)
                        .with(Employee::setStartDate, LocalDate.parse("2020-08-01"))
                        .build(),
                new PojoGenericBuilder<>(Employee::new)
                        .with(Employee::setId, 2L)
                        .with(Employee::setFirstName, "FirstName 2")
                        .with(Employee::setLastName, "LastName 2")
                        .with(Employee::setRole, "Tester")
                        .with(Employee::setSalary, 3200.0)
                        .with(Employee::setStartDate, LocalDate.parse("2020-08-02"))
                        .build());
        assertEquals(expected, result.take(2));
    }

    @Test
    public void testLoadJSonFile() throws Exception
    {
        Dataset<Row> dataSet = spark.read().json(getPath(EMPLOYEESFILE));
        dataSet.show();
        LOGGER.debug("{} columns", Arrays.toString(dataSet.columns()));

        Dataset<Row> rows = dataSet.select("id", "firstName", "lastName", "startDate");
        rows.show();
        LOGGER.debug("Fetched {} filtered rows", rows.collect());

        List<Row> threeRows = rows.takeAsList(3);
        List<Row> expectedThreeRows = asList(RowFactory.create(1, "FirstName 1", "LastName 1", "2020-08-01"),
                RowFactory.create(2, "FirstName 2", "LastName 2", "2020-08-02"),
                RowFactory.create(3, "FirstName 3", "LastName 3", "2020-08-03"));
        assertEquals(expectedThreeRows, threeRows);
    }

    @Test
    public void testLoadFileWithJsonFormat() throws Exception
    {
        Dataset<Row> dataSet = spark.read().format("json").load(getPath(EMPLOYEESFILE));
        dataSet.show();
        LOGGER.debug("{} columns", Arrays.toString(dataSet.columns()));

        Dataset<Row> rows = dataSet.select("firstName", "lastName");
        rows.show();
        LOGGER.debug("Fetched {} filtered rows", rows.collect());

        List<Row> threeRows = rows.takeAsList(3);
        List<Row> expectedThreeRows = asList(RowFactory.create("FirstName 1", "LastName 1"),
                RowFactory.create("FirstName 2", "LastName 2"),
                RowFactory.create("FirstName 3", "LastName 3"));
        assertEquals(expectedThreeRows, threeRows);

        rows.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("employeeName.parquet");
    }

    @Test
    @Disabled
    public void testLoadSequenceFile() throws Exception
    {
    }

    /**
     * Map function to parse employee from json source.
     */
    private static class ParseJson implements FlatMapFunction<Iterator<String>, Employee>
    {
        @Override
        public Iterator<Employee> call(Iterator<String> lines) throws Exception
        {
            List<Employee> people = new ArrayList<>();
            while (lines.hasNext())
            {
                String line = lines.next();
                LOGGER.debug("Parse {} line ", line);
                Employee employee = ObjectMapperUtil.deserialize(Employee.class, line);
                people.add(employee);
            }
            return people.iterator();
        }
    }
}
