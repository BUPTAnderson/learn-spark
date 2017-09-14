package org.training.skew;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by anderson on 17-9-14.
 */
public class AggWordCount
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("AggWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("you,jump", "i,jump", "you,jump","jump,jump", "jump,jump","jump,jump");
        JavaRDD<String> listRDD = sc.parallelize(list);
        JavaRDD<String> flatMap = listRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s)
                    throws Exception
            {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        JavaPairRDD<String, Integer> wordRDD = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s)
                    throws Exception
            {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        /**
         * 给rdd中的每一个key的前缀都打上随机数
         */
        JavaPairRDD<String, Integer> prefixRDD = wordRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2)
                    throws Exception
            {
                Random random = new Random();
                int prefix = random.nextInt(4);
                return new Tuple2<String, Integer>(prefix + "_" + stringIntegerTuple2._1, stringIntegerTuple2._2);
            }
        });

        /**
         * 进行局部聚合
         */
        JavaPairRDD<String, Integer> aggRDD = prefixRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2)
                    throws Exception
            {
                return v1 + v2;
            }
        });

        /**
         * 去除rdd中每个key的前缀
         */
        JavaPairRDD<String, Integer> removePrefixRDD = aggRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2)
                    throws Exception
            {
                String key = stringIntegerTuple2._1.split("_")[1];
                return new Tuple2<String, Integer>(key, stringIntegerTuple2._2);
            }
        });

        /**
         * 进行全局聚合
         */
        removePrefixRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2)
                    throws Exception
            {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2)
                    throws Exception
            {
                System.out.println(stringIntegerTuple2._1 + "--->" + stringIntegerTuple2._2);
            }
        });
    }
}
