package org.training.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by anderson on 17-9-6.
 */
public class TransformationOperation
{
    public static void map() {
        // 创建一个SparkConf对象
        SparkConf conf = new SparkConf();
        // 如果是在本地运行, 设置 setMaster参数为local, 如果不设置默认在集群模式下运行
        conf.setMaster("local");
        // 给任务设置名称
        conf.setAppName("map");
        // 创建程序入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 模拟一个集合, 使用并行化的方式创建RDD
        List<String> list = Arrays.asList("zhangsan", "lisi", "wangwu");
        JavaRDD<String> listRDD = sc.parallelize(list);
//        JavaRDD<String> map = listRDD.map(element -> "hello, " + element);
        // 与下面相同
        JavaRDD<String> map = listRDD.map(new Function<String, String>() {
            @Override
            public String call(String s)
                    throws Exception
            {
                return "hello, " + s;
            }
        });
//        map.foreach(System.out::println);
        // 与下面相同
        map.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s)
                    throws Exception
            {
                System.out.println(s);
            }
        });
    }

    /**
     * 模拟一个集合, 过滤出这个集合里面所有的偶数
     */
    public static void filter() {
        // 创建一个SparkConf对象
        SparkConf conf = new SparkConf();
        // 如果是在本地运行, 设置 setMaster参数为local, 如果不设置默认在集群模式下运行
        conf.setMaster("local");
        // 给任务设置名称
        conf.setAppName("filter");
        // 创建程序入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 模拟一个集合, 使用并行化的方式创建RDD
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        JavaRDD<Integer> filter = listRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer)
                    throws Exception
            {
                return integer % 2 == 0;
            }
        });
        filter.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer)
                    throws Exception
            {
                System.out.println(integer);
            }
        });
    }

    /**
     * 模拟一个集合, {"hello world", "hello spark"}
     * 把每个单词单独拆开, 然后每个单词单独一行
     */
    public static void flatMap() {
        // 创建一个SparkConf对象
        SparkConf conf = new SparkConf();
        // 如果是在本地运行, 设置 setMaster参数为local, 如果不设置默认在集群模式下运行
        conf.setMaster("local");
        // 给任务设置名称
        conf.setAppName("map");
        // 创建程序入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 模拟一个集合, 使用并行化的方式创建RDD
        List<String> list = Arrays.asList("hello,world", "hello,spark");
        JavaRDD<String> listRDD = sc.parallelize(list);
        // 第二个String代表的是iFlatMapFunction这个函数的返回值
        JavaRDD<String> flatMap = listRDD.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s)
                    throws Exception
            {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        flatMap.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s)
                    throws Exception
            {
                System.out.println(s);
            }
        });
    }

    /**
     * 根据key进行分组, 也就是意味着数据是key value的形式
     * Tuple2 对象类似与Java中的map对象
     */
    public static void grouByKey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("groupByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, String>> list = Arrays.asList(new Tuple2<String, String>("北京", "张三"),
                new Tuple2<String, String>("北京", "张三"),
                new Tuple2<String, String>("北京", "李四"),
                new Tuple2<String, String>("上海", "王五"),
                new Tuple2<String, String>("深圳", "马六"),
                new Tuple2<String, String>("深圳", "田七"),
                new Tuple2<String, String>("上海", "朱八"),
                new Tuple2<String, String>("北京", "赵九")
                );
        // 这个地方需要注意一下, 因为我们调用的是ByKey之类的操作, 需要返回pair的形式元素
        JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);

        // 上面的parallelizePairs方法相当于, 下面两个操作
//        JavaRDD<Tuple2<String, String>> tuple2JavaRDD = sc.parallelize(list);
//        JavaPairRDD<String, String> pair = tuple2JavaRDD.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
//            @Override
//            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2)
//                    throws Exception
//            {
//                return new Tuple2<String, String>(stringStringTuple2._1, stringStringTuple2._2);
//            }
//        });
//        JavaPairRDD<String, Iterable<String>> groupByKey = pair.groupByKey();

        JavaPairRDD<String, Iterable<String>> groupByKey = listRDD.groupByKey();
        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> stringIterableTuple2)
                    throws Exception
            {
                System.out.print("address:" + stringIterableTuple2._1 + ", names:");
                stringIterableTuple2._2.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s)
                    {
                        System.out.print(" " + s);
                    }
                });
                System.out.println();
            }
        });
    }

    public static void redueceByKey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("redueceByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("张三", 90),
                new Tuple2<String, Integer>("李四", 100),
                new Tuple2<String, Integer>("张三", 90),
                new Tuple2<String, Integer>("李四", 100),
                new Tuple2<String, Integer>("张三", 90),
                new Tuple2<String, Integer>("李四", 70),
                new Tuple2<String, Integer>("王五", 60),
                new Tuple2<String, Integer>("王五", 90),
                new Tuple2<String, Integer>("王五", 100));
        JavaRDD<Tuple2<String, Integer>> listRDD = sc.parallelize(list);
        JavaPairRDD<String, Integer> mapToPair = listRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2)
                    throws Exception
            {
                return new Tuple2<String, Integer>(stringIntegerTuple2._1, stringIntegerTuple2._2);
            }
        });

        /**
         * Integer, Integer, Integer
         * 前两个Integer是输入类型, 第三个Integer是返回值类型
         * scala.reduceByKey(_ + _)
         */
        mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)
                    throws Exception
            {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2)
                    throws Exception
            {
                System.out.println(stringIntegerTuple2._1 + ": " + stringIntegerTuple2._2);
            }
        });
    }

    public static void sortByKey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("sortByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> list = Arrays.asList(
                new Tuple2<Integer, String>(190, "张三"),
                new Tuple2<Integer, String>(168, "李四"),
                new Tuple2<Integer, String>(170, "王五"),
                new Tuple2<Integer, String>(175, "马六"),
                new Tuple2<Integer, String>(180, "田七"),
                new Tuple2<Integer, String>(178, "朱八"),
                new Tuple2<Integer, String>(174, "赵九"),
                new Tuple2<Integer, String>(181, "赵九"));
        JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list);
        // false是降序, true是升序
        listRDD.sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2)
                    throws Exception
            {
                System.out.println(integerStringTuple2._2 + ", high:" + integerStringTuple2._1);
            }
        });
    }
    public static void main(String[] args)
    {
        // Transformations操作
//        map();
//        filter();
//        flatMap();
//        grouByKey();
//        redueceByKey();
        sortByKey();
    }
}
