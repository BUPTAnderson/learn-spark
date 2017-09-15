package org.training.mapjoin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by anderson on 17-9-14.
 */
public class ExtendRDDTest
{
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("ExtendRDDTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> list1 = Arrays.asList(
                new Tuple2<String, String>("001", "令狐冲"),
                new Tuple2<String, String>("002", "任莹莹")
        );
        List<Tuple2<String, String>> list2 = Arrays.asList(
                new Tuple2<String, String>("001", "一班"),
                new Tuple2<String, String>("002", "二班")
        );
        JavaRDD<Tuple2<String, String>> list1RDD = sc.parallelize(list1);
        JavaRDD<Tuple2<String, String>> list2RDD = sc.parallelize(list2);
        /**
         * 首先将其中key分布比较均匀的RDD扩容100倍
         *
         */
        JavaPairRDD<String, String> extendRDD = list1RDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,String>, String, String>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, String> stringStringTuple2)
                    throws Exception
            {
                ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                for (int i = 0; i < 100; i++) {
                    list.add(new Tuple2<String, String>(i + "_" + stringStringTuple2._1, stringStringTuple2._2));
                }
                return list.iterator();
            }
        });
        /**
         * 将另外一个key分布不均匀的RDD打上0~99的随机数
         */
        JavaPairRDD<String, String> mappedRDD = list2RDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2)
                    throws Exception
            {
                Random random = new Random();
                int prefix = random.nextInt(100);
                return new Tuple2<String, String>(prefix + "_" + stringStringTuple2._1, stringStringTuple2._2);
            }
        });
        mappedRDD.join(extendRDD)
                .foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {
                    @Override
                    public void call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2)
                            throws Exception
                    {
                        System.out.println(stringTuple2Tuple2._1.split("_")[1] + " " + stringTuple2Tuple2._2._1 + " " + stringTuple2Tuple2._2._2);
                    }
                });
    }
}
