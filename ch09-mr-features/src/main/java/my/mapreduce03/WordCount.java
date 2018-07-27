package my.mapreduce03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by xueyong.cui on 2018/7/26.
 */
public class WordCount {

    enum MyWordCounter{COUNT_LINES,COUNT_WORD}
//  enum Weekday{MONDAY, TUESDAY, WENSDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY}

    public static void main(String[] args) throws Exception {
        // 指定hdfs相关的参数
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar包所在路径
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        // 指定reducetask的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 本地路径
        Path inputPath = new Path("d:/wordcount/input");
        Path outputPath = new Path("d:/wordcount/output");

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 最后提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }

    private static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

//          COUNT_LINES++;
            context.getCounter(MyWordCounter.COUNT_LINES).increment(1L);

            // 在此写maptask的业务代码
            String[] words = value.toString().split(" ");
            for(String word: words){
                context.write(new Text(word), new LongWritable(1));
                context.getCounter(MyWordCounter.COUNT_WORD).increment(1L);
            }
        }
    }

    private static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // 在此写reducetask的业务代码
            long sum = 0;
            for(LongWritable v: values){
                sum += v.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
