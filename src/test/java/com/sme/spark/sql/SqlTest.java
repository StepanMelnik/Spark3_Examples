package com.sme.spark.sql;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sme.spark.ASparkTest;
import com.sme.spark.model.Employee;
import com.sme.spark.util.PojoGenericBuilder;

/**
 * Unit tests of {@link Dataset} data.
 */
public class SqlTest extends ASparkTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTest.class);
    private static final String EMPLOYEESFILE = "employees_per_line.json";

    @Test
    public void testDataSetOperationsTest() throws Exception
    {
        Dataset<Row> dataSet = spark.read().format("json").load(getPath(EMPLOYEESFILE));
        dataSet.createOrReplaceTempView("employees");
        dataSet.show();
        dataSet.printSchema();
        LOGGER.debug("{} columns", Arrays.toString(dataSet.columns()));

        dataSet.select("firstName", "lastName", "startDate", "role", "salary").show();
        dataSet.groupBy("salary").count().show();

        Dataset<Row> result = dataSet
                .select("firstName", "lastName", "startDate", "role", "salary")
                .filter(dataSet.col("salary").geq(6000.0));

        List<Row> expected = asList(RowFactory.create("FirstName 29", "LastName 29", "2020-08-29", "Dev", 6000.0),
                RowFactory.create("FirstName 30", "LastName 30", "2020-08-30", "Dev", 6100.0));
        List<Row> actual = result.collectAsList();

        assertEquals(expected, actual);
    }

    @Test
    void testSqlQueries() throws Exception
    {
        Dataset<Row> dataSet = spark.read().format("json").load(getPath(EMPLOYEESFILE));
        dataSet.createOrReplaceTempView("employees");
        dataSet.show();
        dataSet.printSchema();
        LOGGER.debug("{} columns", Arrays.toString(dataSet.columns()));

        Dataset<Row> rows = dataSet.sqlContext().sql("SELECT firstName, lastName, startDate, role, salary FROM employees WHERE salary >= 6000.0 ORDER BY id LIMIT 7");
        rows.show();

        List<Row> expected = asList(RowFactory.create("FirstName 29", "LastName 29", "2020-08-29", "Dev", 6000.0),
                RowFactory.create("FirstName 30", "LastName 30", "2020-08-30", "Dev", 6100.0));
        List<Row> actual = rows.collectAsList();

        assertEquals(expected, actual);
    }

    @Test
    void testInsertEmployeesData() throws Exception
    {
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        //        Dataset<Employee> employeeDataSetByEncoder = spark.read().json(getPath(EMPLOYEESFILE)).as(employeeEncoder);
        //        employeeDataSetByEncoder.show();
        //
        //        assertEquals(20, employeeDataSetByEncoder.collectAsList().size());

        // Insert employees
        List<Employee> employees = asList(
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

        Dataset<Employee> dataSet = spark.createDataset(unmodifiableList(employees), employeeEncoder);
        dataSet.show();
        assertEquals(employees, dataSet.collectAsList());
    }

    @Test
    void testInsertIntegerData() throws Exception
    {
        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> integerDataSet = spark.createDataset(asList(1, 2, 3, 5, 7, 9), integerEncoder);
        integerDataSet.show();

        assertEquals(asList(1, 2, 3, 5, 7, 9), integerDataSet.collectAsList());
    }
}
