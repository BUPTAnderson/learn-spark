package org.training.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by anderson on 17-9-7.
 */
public class ActionOperation
{
    public static void reduce() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("reduce");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        // reduce是一个action操作
        Integer reduce = listRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)
                    throws Exception
            {
                return integer + integer2;
            }
        });
        System.out.println(reduce);
    }

    // collect操作容易造成内存溢出
    public static void collect() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("collect");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(3, 2, 3, 9);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
        List<Integer> collect = union.collect();
        collect.forEach(System.out::println);
    }

    // topN算法
    public static void take() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("take");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        List<Integer> take =  listRDD.take(3);
        take.forEach(System.out::println);
    }

    public static void count() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        long count = listRDD.count();
        System.out.println(count);
    }

    // 排序取TopN
    public static void takeOrdered() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("takeOrdered");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 222, 3, 40, 500, 6, 71, 18, 9, 100);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        // 有一个重载的方法, 可以传入比较器Comparator, 默认是升序排列
        List<Integer> takeOrdered = listRDD.takeOrdered(3);
        takeOrdered.forEach(System.out::println);

        List<Integer> top = listRDD.top(3);
        top.forEach(System.out::println);
    }

    public static void saveAsTextFile() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("saveAsTextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(3, 2, 3, 9);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
        // 报存到一个目录下面
        union.saveAsTextFile("/tmp/union");
    }
    public static void main(String[] args)
    {
//        reduce();
//        collect();
//        take();
//        count();
//        takeOrdered();
        saveAsTextFile();
    }
}
