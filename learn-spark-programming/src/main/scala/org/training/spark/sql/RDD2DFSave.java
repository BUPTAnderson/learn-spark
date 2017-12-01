package org.training.spark.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Created by anderson on 17-12-1.
 */
public class RDD2DFSave
{
    public static String hdfsUrl = "hdfs://anderson-JD:9000";
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("RDD2DFSave");
        sparkConf.setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();

//        try {
//            new FeatureExtractor().extractFeature(sparkContext, 1);
//        }
//        catch (Exception e) {
//            e.printStackTrace();
//        }

        JavaRDD<String> personRDD = sparkContext.textFile("hdfs://anderson-JD:9000/examples/kv1.txt");
        JavaRDD<String> filtedRDD = personRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1)
                    throws Exception
            {
                if (v1 == null) {
                    return false;
                }
                return true;
            }
        });
        Long size = filtedRDD.map(new Function<String, Long>()
        {
            public Long call(String message)
                    throws Exception
            {
                return Long.valueOf(message.length() + 1);
            }
        }).reduce(new Function2<Long, Long, Long>()
        {
            public Long call(Long integer, Long integer2)
                    throws Exception
            {
                return integer + integer2;
            }
        });
        System.out.println("process size:" + size);
        String suffix = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        System.out.println("suffix:" + suffix);
        filtedRDD.repartition(1).saveAsTextFile("hdfs://anderson-JD:9000/" + "/datajingdo_m" + "/teststream/" +suffix);

        Configuration config = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsUrl), config);
            Path srcPath = new Path("hdfs://anderson-JD:9000/" + "/datajingdo_m" + "/teststream/" +suffix);
            FileStatus[] files = fs.listStatus(srcPath);
            System.out.println("file length:" + files.length);
            for (FileStatus file : files) {
                Path filePath = file.getPath();
                System.out.println(filePath.toString());
                if (filePath.getName().equals("_SUCCESS")) {
                    continue;
                }

                FSDataInputStream fsdInputStream = fs.open(filePath);
                String destDir = "/home/anderson/zzz.txt";
                FileOutputStream fos = new FileOutputStream(destDir);
                byte[] buff = new byte[1024];
                int readCount = 0;
                int readSize = 0;
                readCount = fsdInputStream.read(buff);
                while (readCount != -1) {
                    readSize += readCount;
                    fos.write(buff, 0, readCount);
                    readCount = fsdInputStream.read(buff);
                }
                System.out.println("read over, read size:" + readSize);
                fsdInputStream.close();
                Thread.currentThread().sleep(60000L);
                // 删除目录
                fs.delete(srcPath, true);
                System.out.println("srcPath has been deleted!");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

//        String schemaString = "message";
//        List<StructField> fields = new ArrayList<>();
//        StructField field = DataTypes.createStructField(schemaString, DataTypes.StringType, true);
//        fields.add(field);
//        StructType schema = DataTypes.createStructType(fields);
//
//        JavaRDD<Row> rowRDD = personRDD.map(new Function<String, Row>()
//        {
//            @Override
//            public Row call(String record)
//                    throws Exception
//            {
//                return RowFactory.create(record);
//            }
//        });
//        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
//        peopleDataFrame.write().mode(SaveMode.Append).format("orc").save("hdfs://anderson-JD:9000/examples/iptables_new/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
    }
}

class FeatureExtractor
        implements Serializable
{
    public void extractFeature(JavaSparkContext sc, int repartitionNum)
            throws Exception
    {
        String hdfsPath = "hdfs://anderson-JD:9000/examples/iptables_new/20171201115005"; //存放原始数据的文件
        //Spark可以读取单独的一个文件或整个目录
        JavaRDD<String> rddx = sc.textFile(hdfsPath).repartition(repartitionNum);
        rddx = rddx.map(new ExtractFeatureMap());

        //写入hdfs文件位置
        String destinationPath = "hdfs://anderson-JD:9000/examples/iptables_new/" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        //创建Hdfs文件，打开Hdfs输出流
        HdfsOperate.openHdfsFile(destinationPath);

        //分块读取RDD数据并保存到hdfs
        //如果直接用collect()函数获取List<String>，可能因数据量过大超过内存空间而失败
        for (int i = 0; i < repartitionNum; i++) {
            int[] index = new int[1];
            index[0] = i;
            List<String>[] featureList = rddx.collectPartitions(index);
            if (featureList.length != 1) {
                System.out.println("[FeatureExtractor]>> featureList.length is not 1!");
            }
            for (String str : featureList[0]) {
                //写一行到Hdfs文件
                HdfsOperate.writeString(str);
            }
        }
        //关闭Hdfs输出流
        HdfsOperate.closeHdfsFile();
    }

class ExtractFeatureMap
            implements Function<String, String>
    {
        @Override
        public String call(String line)
                throws Exception
        {
            try {
                //TODO:你自己的操作，返回String类型
                return line;
            }
            catch (Exception e) {
                System.out.println("[FeatureExtractor]>>GetTokenAndKeywordFeature error:" + e);
            }
            return null;
        }
    }
}

class HdfsOperate implements Serializable
{

    private static Configuration conf = new Configuration();
    private static BufferedWriter writer = null;

    //在hdfs的目标位置新建一个文件，得到一个输出流
    public static void openHdfsFile(String path)
            throws Exception
    {
        FileSystem fs = FileSystem.get(URI.create(path), conf);
        writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
        if (null != writer) {
            System.out.println("[HdfsOperate]>> initialize writer succeed!");
        }
    }

    //往hdfs文件中写入数据
    public static void writeString(String line)
    {
        try {
            writer.write(line + "\n");
        }
        catch (Exception e) {
            System.out.println("[HdfsOperate]>> writer a line error:" + e);
        }
    }

    //关闭hdfs输出流
    public static void closeHdfsFile()
    {
        try {
            if (null != writer) {
                writer.close();
                System.out.println("[HdfsOperate]>> closeHdfsFile close writer succeed!");
            }
            else {
                System.out.println("[HdfsOperate]>> closeHdfsFile writer is null");
            }
        }
        catch (Exception e) {
            System.out.println("[HdfsOperate]>> closeHdfsFile close hdfs error:" + e);
        }
    }
}