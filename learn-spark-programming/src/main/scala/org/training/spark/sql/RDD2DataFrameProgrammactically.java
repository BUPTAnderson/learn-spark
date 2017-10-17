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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by anderson on 17-9-16.
 */
public class RDD2DataFrameProgrammactically
{
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDD2DataFrameProgrammactically");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

        JavaRDD<String> personRDD = sparkContext.textFile("hdfs://anderson-JD:9000/examples/people.txt");

        /**
         * 是从数据库里面获取出来的
         * 在实际的开发中我们需要写另外的代码去获取
         */
        String schemaString = "name age";
        // create schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            // 最后一个参数是是否为null
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = personRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record)
                    throws Exception
            {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        // Creates a temporary view using the DataFrame
        peopleDataFrame.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenageNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name:" + row.getString(0) + ", age:" + row.getInt(1), stringEncoder);

        teenageNamesByIndexDF.show();
    }
}
