package org.training.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by anderson on 17-9-16.
 */
public class RDD2DataFrameReflection
{
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf();
//        conf.setMaster("local");
        sparkConf.setAppName("RDD2DataFrameReflection");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession
                .builder()
//                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
                .config(sparkConf)
                .getOrCreate();

        JavaRDD<Person> personRDD = sparkContext.textFile("hdfs://anderson-JD:9000/examples/people.txt").map(new Function<String, Person>() {
            @Override
            public Person call(String line)
                    throws Exception
            {
                String[] strs = line.split(",");
                String name = strs[0];
                int age = Integer.parseInt(strs[1].trim());
                Person person = new Person(age, name);
                return person;
            }
        });
        // rdd -> dataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(personRDD, Person.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");
        Dataset<Row> teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenageNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name:" + row.getString(0) + ", age:" + row.getInt(1), stringEncoder);

        teenageNamesByIndexDF.show();
        // 保存成文件
//        teenageNamesByIndexDF.javaRDD().saveAsTextFile("hdfs://anderson-JD:9000/examples/result");
    }
}
