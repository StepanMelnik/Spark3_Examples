package com.sme.spark.sql;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

import com.sme.spark.ASparkTest;
import com.sme.spark.model.Employee;
import com.sme.spark.util.PojoGenericBuilder;

/**
 * Unit tests of Parquet DataSource.
 */
public class ParquetDataSourceTest extends ASparkTest
{
    @Test
    void testSaveLoad() throws Exception
    {
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
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

        dataSet
                .select("firstName", "lastName", "startDate")
                .write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("short_employees.parquet");

        URI uri = ASparkTest.class.getClassLoader().getResource("employees_per_line.json").toURI();
        String parquetPath = Paths.get(uri).getParent().getParent().getParent().toFile().getAbsolutePath();

        Dataset<Row> shortEmployees = spark.sql("SELECT * FROM parquet.`" + parquetPath + "/short_employees.parquet`");

        List<Row> loadedList = shortEmployees.collectAsList();
        List<Row> expected = asList(RowFactory.create("FirstName 1", "LastName 1", Date.valueOf(LocalDate.parse("2020-08-01"))),
                RowFactory.create("FirstName 2", "LastName 2", Date.valueOf(LocalDate.parse("2020-08-02"))));

        assertEquals(expected, loadedList);
    }
}
