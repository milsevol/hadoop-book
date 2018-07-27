package my.mapreduce01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by xueyong.cui on 2018/7/26.
 */
public class WordCountMR {
    /**
     * 该main方法是该mapreduce程序运行的入口，其中用一个Job类对象来管理程序运行时所需要的很多参数：
     * 比如，指定用哪个组件作为数据读取器、数据结果输出器
     *     指定用哪个类作为map阶段的业务逻辑类，哪个类作为reduce阶段的业务逻辑类
     *     指定wordcount job程序的jar包所在路径
     *     ....
     *     以及其他各种需要的参数
     */
    public static void main(String[] args) throws Exception {
        // 指定hdfs相关的参数
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
        System.setProperty("HADOOP_USER_NAME", "hadoop");

//      conf.set("mapreduce.framework.name", "yarn");
//      conf.set("yarn.resourcemanager.hostname", "hadoop04");

        Job job = Job.getInstance(conf);

        // 设置jar包所在路径
        job.setJarByClass(WordCountMR.class);

        // 指定mapper类和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 指定maptask的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定reducetask的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//      Path inputPath = new Path("d:/wordcount/input");
//      Path outputPath = new Path("d:/wordcount/output");

        // 指定该mapreduce程序数据的输入和输出路径
        Path inputPath = new Path("/wordcount/input");
        Path outputPath = new Path("/wordcount/output");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

//      job.submit();
        // 最后提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }

    /**
     * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     *
     * KEYIN 是指框架读取到的数据的key的类型，在默认的InputFormat下，读到的key是一行文本的起始偏移量，所以key的类型是Long
     * VALUEIN 是指框架读取到的数据的value的类型,在默认的InputFormat下，读到的value是一行文本的内容，所以value的类型是String
     * KEYOUT 是指用户自定义逻辑方法返回的数据中key的类型，由用户业务逻辑决定，在此wordcount程序中，我们输出的key是单词，所以是String
     * VALUEOUT 是指用户自定义逻辑方法返回的数据中value的类型，由用户业务逻辑决定,在此wordcount程序中，我们输出的value是单词的数量，所以是Integer
     *
     * 但是，String ，Long等jdk中自带的数据类型，在序列化时，效率比较低，hadoop为了提高序列化效率，自定义了一套序列化框架
     * 所以，在hadoop的程序中，如果该数据需要进行序列化（写磁盘，或者网络传输），就一定要用实现了hadoop序列化框架的数据类型
     *
     * Long ----> LongWritable
     * String ----> Text
     * Integer ----> IntWritable
     * Null ----> NullWritable
     */
    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {

            String[] words = value.toString().split(" ");
            for(String word: words){
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    /**
     * 首先，和前面一样，Reducer类也有输入和输出，输入就是Map阶段的处理结果，输出就是Reduce最后的输出
     * reducetask在调我们写的reduce方法,reducetask应该收到了前一阶段（map阶段）中所有maptask输出的数据中的一部分
     * （数据的key.hashcode%reducetask数==本reductask号），所以reducetaks的输入类型必须和maptask的输出类型一样
     *
     * reducetask将这些收到kv数据拿来处理时，是这样调用我们的reduce方法的：
     * 先将自己收到的所有的kv对按照k分组（根据k是否相同）
     * 将某一组kv中的第一个kv中的k传给reduce方法的key变量，把这一组kv中所有的v用一个迭代器传给reduce方法的变量values
     */
    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable v: values){
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
