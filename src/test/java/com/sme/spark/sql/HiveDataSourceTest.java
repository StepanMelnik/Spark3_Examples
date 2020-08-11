package com.sme.spark.sql;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sme.spark.model.Employee;
import com.sme.spark.model.KeyValueRow;
import com.sme.spark.util.PojoGenericBuilder;

/**
 * Unit tests of Hive DataSource.
 */
public class HiveDataSourceTest
{
    private SparkSession spark;

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
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        spark.stop();
    }

    @Test
    void testHive() throws Exception
    {
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        List<Employee> employees = createEmployees();

        spark.sql("CREATE TABLE IF NOT EXISTS employees (key INT, value STRING) USING hive");

        Dataset<Employee> dataSet = spark.createDataset(unmodifiableList(employees), employeeEncoder);
        dataSet.show();

        dataSet.createOrReplaceTempView("employees");

        Dataset<Row> list = spark.sql("SELECT * FROM employees WHERE id >= 1 ORDER BY id").cache();
        list.show();

        Dataset<Row> count = spark.sql("SELECT COUNT(*) FROM employees");
        List<Row> countList = count.collectAsList();

        assertEquals(2, countList.get(0).getLong(0));
    }

    @Test
    void testHiveTablesJoin() throws Exception
    {
        // create KeyValueRow table
        List<KeyValueRow> rows = IntStream.range(1, 5)
                .boxed()
                .map(i -> new KeyValueRow(i, "value" + i))
                .collect(toList());

        Dataset<Row> keyValueRowDataset = spark.createDataFrame(rows, KeyValueRow.class);
        keyValueRowDataset.createOrReplaceTempView("KeyValueRow");

        spark.sql("SELECT * FROM KeyValueRow WHERE key >= 0 ORDER BY key").show();

        // create Employee table
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);

        //Dataset<Row> employeeRowDataset = spark.createDataFrame(createEmployees(), Employee.class); // dows not work, because of java.lang.ClassCastException: java.time.LocalDate cannot be cast to java.sql.Date
        Dataset<Employee> employeeRowDataset = spark.createDataset(unmodifiableList(createEmployees()), employeeEncoder);
        employeeRowDataset.createOrReplaceTempView("Employee");

        spark.sql("SELECT * FROM Employee WHERE id >= 0 ORDER BY id").show();

        // Inner Join tables in Hive
        Dataset<Row> datasetResult = spark.sql("SELECT e.id, e.firstName, e.lastName, r.value FROM Employee e JOIN KeyValueRow r ON r.key = e.id").cache();
        datasetResult.show();

        List<Row> result = datasetResult.collectAsList();
        List<Row> expected = asList(RowFactory.create(1, "FirstName 1", "LastName 1", "value1"),
                RowFactory.create(2, "FirstName 2", "LastName 2", "value2"));
        assertEquals(expected, result);
    }

    private List<Employee> createEmployees()
    {
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
        return employees;
    }
}
