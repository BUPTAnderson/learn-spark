package org.training.spark.core;

import org.apache.spark.Partitioner;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
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

    public static void join() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> listname = Arrays.asList(
                new Tuple2<Integer, String>(1, "张三"),
                new Tuple2<Integer, String>(2, "李四"),
                new Tuple2<Integer, String>(3, "王五"),
                new Tuple2<Integer, String>(4, "马六"),
                new Tuple2<Integer, String>(5, "田七"),
                new Tuple2<Integer, String>(6, "朱八"),
                new Tuple2<Integer, String>(7, "赵九"),
                new Tuple2<Integer, String>(8, "钱十"));
        List<Tuple2<Integer, Integer>> listscore = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 80),
                new Tuple2<Integer, Integer>(6, 70),
                new Tuple2<Integer, Integer>(4, 95),
                new Tuple2<Integer, Integer>(5, 98),
                new Tuple2<Integer, Integer>(8, 68),
                new Tuple2<Integer, Integer>(7, 70));
        JavaPairRDD<Integer, String> listnameRDD = sc.parallelizePairs(listname);
        JavaPairRDD<Integer, Integer> listscoreRDD = sc.parallelizePairs(listscore);
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = listnameRDD.join(listscoreRDD);
        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> integerTuple2Tuple2)
                    throws Exception
            {
                System.out.print("学号:" + integerTuple2Tuple2._1);
                System.out.print(", 姓名:" + integerTuple2Tuple2._2._1);
                System.out.println(", 分数:" + integerTuple2Tuple2._2._2);
            }
        });
    }

    /**
     * Cogroup:这个实现根据两个要进行合并的两个RDD操作,生成一个CoGroupedRDD的实例,这个RDD的返回结果是把相同的key中两个RDD分别进行合并操作,
     * 最后返回的RDD的value是一个Pair的实例,这个实例包含两个Iterable的值,第一个值表示的是RDD1中相同KEY的值,第二个值表示的是RDD2中相同key的值.
     */
    public static void cogroup() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("cogroup");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<Integer, String>> listname = Arrays.asList(
                // <班级, 姓名>
                new Tuple2<Integer, String>(1, "张三"),
                new Tuple2<Integer, String>(2, "李四"),
                new Tuple2<Integer, String>(3, "王五"),
                new Tuple2<Integer, String>(4, "马六"),
                new Tuple2<Integer, String>(1, "田七"),
                new Tuple2<Integer, String>(2, "朱八"),
                new Tuple2<Integer, String>(3, "赵九"),
                new Tuple2<Integer, String>(4, "赵九"));
        List<Tuple2<Integer, Integer>> listscore = Arrays.asList(
                // <班级, 男生or女生人数>
                new Tuple2<Integer, Integer>(1, 30),
                new Tuple2<Integer, Integer>(2, 22),
                new Tuple2<Integer, Integer>(3, 23),
                new Tuple2<Integer, Integer>(6, 33),
                new Tuple2<Integer, Integer>(4, 30),
                new Tuple2<Integer, Integer>(1, 21),
                new Tuple2<Integer, Integer>(2, 18),
                new Tuple2<Integer, Integer>(3, 31),
                new Tuple2<Integer, Integer>(4, 30));
        JavaPairRDD<Integer, String> listnameRDD = sc.parallelizePairs(listname);
        JavaPairRDD<Integer, Integer> listscoreRDD = sc.parallelizePairs(listscore);
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = listnameRDD.cogroup(listscoreRDD);
        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2Tuple2)
                    throws Exception
            {
                System.out.print("班级:" + integerTuple2Tuple2._1 + ", 班长:");
                integerTuple2Tuple2._2._1.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s)
                    {
                        System.out.print(" " + s);
                    }
                });
                System.out.print(" 男女生人数:");
                integerTuple2Tuple2._2._2.forEach(new Consumer<Integer>() {
                    @Override
                    public void accept(Integer integer)
                    {
                        System.out.print(" " + integer);
                    }
                });
                System.out.println();
            }
        });
    }

    /**
     * 求rdd并集, 但是不去重
     */
    public static void union() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("union");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(3, 2, 3, 9);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        JavaRDD<Integer> union = listaRDD.union(listbRDD);
        union.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer)
                    throws Exception
            {
                System.out.println(integer);
            }
        });
    }

    /**
     * 求两个RDD的交集
     */
    public static void intersection() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("intersection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<Integer> listb = Arrays.asList(3, 2, 3, 9);
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<Integer> listbRDD = sc.parallelize(listb);
        listaRDD.intersection(listbRDD).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer)
                    throws Exception
            {
                System.out.println(integer);
            }
        });
    }

    /**
     * 去重
     */
    public static void distinct() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("distinct");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 3, 2, 5, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        listRDD.distinct().foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer)
                    throws Exception
            {
                System.out.println(integer);
            }
        });
    }

    /**
     * 求两个RDD的笛卡尔积
     */
    public static void cartesian() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("cartesian");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> lista = Arrays.asList(1, 2, 3, 4);
        List<String> listb = Arrays.asList("a", "b", "c", "d");
        JavaRDD<Integer> listaRDD = sc.parallelize(lista);
        JavaRDD<String> listbRDD = sc.parallelize(listb);
        listaRDD.cartesian(listbRDD).foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2)
                    throws Exception
            {
                System.out.println(integerStringTuple2._1 + ", " + integerStringTuple2._2);
            }
        });
    }

    public static void mapPartitions() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("mapPartitions");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        // 强制指定分区数为2, 默认是1个分区
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        listRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, String>() {
            /**
             * 这里每次处理的就是一个分区的数
             * @param integerIterator
             * @return
             * @throws Exception
             */
            @Override
            public Iterator<String> call(Iterator<Integer> integerIterator)
                    throws Exception
            {
                System.out.println("---------------------------");
                ArrayList<String> list = new ArrayList<String>();
                while (integerIterator.hasNext()) {
                    Integer i  = integerIterator.next();
                    list.add("hello " + i);
                }
                return list.iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s)
                    throws Exception
            {
                System.out.println(s);
            }
        });
    }

    /**
     * filter过滤之后 --> partition数据量会减少
     * 100 partition task
     * 100 -> 50 partition task
     *
     * 这一个repartition分区, 会进行shuffle操作
     */
    public static void repartition() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("repartition");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        listRDD.repartition(2).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer)
                    throws Exception
            {
                System.out.println(integer);
            }
        });
    }

    /**
     * repartition起始只是coalesce的shuffle为true的建议的实现, repartiton的shuffle默认是true
     */
    public static void coalesce() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("coalesce");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        // 默认一个分区
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        /**
         * numPartitons 重新分成几个区, 这里重新分区成2个分区
         * shuffle 是否进行shuffle
         */
        listRDD.coalesce(2, true);
        /**
         如果原来有N个partition，需要重新规划成M个partition
          1）N < M  需要将shuffle设置为true。
          2）N > M 相差不多，N=1000 M=100  建议 shuffle=false 。
          父RDD和子RDD是窄依赖
          3）N >> M  比如 n=100 m=1  建议shuffle设置为true，这样性能更好。
         */
    }

    /**
     * 随机采样
     */
    public static void sample() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("sample");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        /**
         * 第一个参数withReplacement是true表示有放回取样, false表示无放回.
         * 第二个参数表示比例, 这个比例不是精确的, 比如这里是0.2, 可能打印出2个或3个甚至4个数
         */
        listRDD.sample(true, 0.2).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer)
                    throws Exception
            {
                System.out.println("------------------------------" + integer);
            }
        });
    }

    /**
     * 做一个单词计数的例子
     */
    public static void aggregateByKey() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("aggregateByKey");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("hello,world", "hello,spark");
        JavaRDD<String> listRDD = sc.parallelize(list);
        listRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s)
                    throws Exception
            {
                return Arrays.asList(s.split(",")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s)
                    throws Exception
            {
                return new Tuple2<String, Integer>(s, 1);
            }
        })
        /**
         * 其实reduceByKey就是aggregateByKey的简化版, 就是aggregateByKey多提供了一个函数(第二个参数),
         * 类似于MapReduce的combine操作(就在map端执行reduce的操作)
         * 第一个参数代表的是每个key的初始值
         * 第二个参数是一个函数, 类似于map-side的本地聚合
         * 第三个参数也是一个函数类似于reduce全局聚合
         */
        .aggregateByKey(0, new Function2<Integer, Integer, Integer>()
        {
            @Override
            public Integer call(Integer integer, Integer integer2)
                    throws Exception
            {
                return integer + integer2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)
                    throws Exception
            {
                return integer + integer2;
            }
        });

    }

    public static void mapPartitionsWithIndex() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("mapPartitionsWithIndex");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);
        // 第二个参数代表是否返回partition的下标, true表示返回
        listRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            // 第一个参数index是分区的索引
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> iterator)
                    throws Exception
            {
                ArrayList<String> list1 = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String result = iterator.next() + "_" + index;
                    list1.add(result);
                }
                return list1.iterator();
            }
        }, true)
        .foreach(new VoidFunction<String>() {
            @Override
            public void call(String s)
                    throws Exception
            {
                System.out.println(s);
            }
        });
    }

    public static void repartitionAndSortWithinPartitions() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("repartitionAndSortWithinPartitions");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = Arrays.asList(11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        Random random = new Random();
        JavaPairRDD<Integer, Integer> mapToPair = listRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i)
                    throws Exception
            {
                return new Tuple2<Integer, Integer>(i, random.nextInt(10));
            }
        });
        JavaPairRDD<Integer, Integer> repartitionAndSortWithinPartitions = mapToPair.repartitionAndSortWithinPartitions(new Partitioner() {
            // 要分为几个分区
            @Override
            public int numPartitions()
            {
                return 2;
            }

            @Override
            public int getPartition(Object key)
            {
                return key.toString().hashCode() % 2;
            }
        });
        repartitionAndSortWithinPartitions.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> integerIntegerTuple2)
                    throws Exception
            {
                System.out.println(integerIntegerTuple2._1 + ": " + integerIntegerTuple2._2);
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
//        sortByKey();
//        join();
//        cogroup();
//        union();
//        intersection();
//        distinct();
//        cartesian();
//        mapPartitions();
//        repartition();
//        sample();
//        mapPartitionsWithIndex();
        repartitionAndSortWithinPartitions();
    }
}
