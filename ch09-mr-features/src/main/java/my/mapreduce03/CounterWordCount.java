package my.mapreduce03;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by 000 on 2018/7/26.
 */
public class CounterWordCount {
    enum CouterWordCountC{COUNT_WORDS, COUNT_LINES}
    public static void main(String[] args) throws Exception {
        // 指定 hdfs 相关的参数
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置 jar 包所在路径
        job.setJarByClass(CounterWordCount.class);
        job.setMapperClass(WCCounterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 本地路径
        Path inputPath = new Path("d:/wordcount/input");
        FileInputFormat.setInputPaths(job, inputPath);
        job.setNumReduceTasks(0);
        Path outputPath = new Path("d:/wordcount/output");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        // 最后提交任务
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion?0:1);
    }
    private static class WCCounterMapper extends Mapper<LongWritable, Text, Text,
            LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 统计行数，因为默认读取文本是逐行读取，所以 map 执行一次，行数+1
            context.getCounter(CouterWordCountC.COUNT_LINES).increment(1L);
            String[] words = value.toString().split(" ");
            for(String word: words){
            // 统计单词总数，遇见一个单词就+1
                context.getCounter(CouterWordCountC.COUNT_WORDS).increment(1L);
            }
        }
    }
}
