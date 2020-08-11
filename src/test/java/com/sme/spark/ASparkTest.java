package com.sme.spark;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Abstract implementation of unit spark tests.
 */
public abstract class ASparkTest
{
    protected SparkSession spark;
    protected JavaSparkContext javaSparkContext;

    @BeforeEach
    public void setUp() throws Exception
    {
        spark = SparkSession
                .builder()
                .appName(this.getClass().getSimpleName())
                //.master("local[*]") // https://spark.apache.org/docs/latest/submitting-applications.html
                .master("local[4]") // Run Spark locally with as four worker threads
                .getOrCreate();

        javaSparkContext = new JavaSparkContext(spark.sparkContext());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
        spark.stop();
    }

    /**
     * Fetch an absolute path of file in class loader.
     * 
     * @param fileName The file name;
     * @return Returns absolute path of file.
     */
    protected String getPath(String fileName)
    {
        return ASparkTest.class.getClassLoader().getResource(fileName).getFile();
    }

    /**
     * Fetch an absolute parent path of file in class loader.
     * 
     * @param fileName The file name;
     * @return Returns absolute path of file.
     */
    protected String getParentPath(String fileName)
    {
        try
        {
            URI uri = ASparkTest.class.getClassLoader().getResource(fileName).toURI();
            return Paths.get(uri).getParent().toFile().getAbsolutePath();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException("Cannot fetch file by class loader: " + fileName);
        }
    }
}
