package my.mapreduce03;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class ScoreTop1MR {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ScoreTop1MR.class);
        job.setMapperClass(ScoreTop1MRMapper.class);
        job.setReducerClass(ScoreTop1MRReducer.class);
        job.setOutputKeyClass(ClazzScore.class);
        job.setOutputValueClass(DoubleWritable.class);
     // 设置传入 reducer 的数据分组规则
        job.setGroupingComparatorClass(ClazzScoreGroupComparator.class);
        FileInputFormat.setInputPaths(job, "d:/score_all/input");
        Path p = new Path("d:/score_all/output1");
        FileSystem fs = FileSystem.newInstance(conf);
        if(fs.exists(p)){
            fs.delete(p, true);
        }
        FileOutputFormat.setOutputPath(job, p);
        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }
    static class ScoreTop1MRMapper extends Mapper<LongWritable, Text, ClazzScore,
            DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            String[] splits = value.toString().split("\t");
            ClazzScore cs = new ClazzScore(splits[0], Double.parseDouble(splits[2]));
            context.write(cs, new DoubleWritable(Double.parseDouble(splits[2])));
        }
    }
    static class ScoreTop1MRReducer extends Reducer<ClazzScore, DoubleWritable, ClazzScore,
            DoubleWritable>{
        @Override
        protected void reduce(ClazzScore cs, Iterable<DoubleWritable> scores, Context
                context) throws IOException, InterruptedException {
            // 按照规则，取每组的第一个就是 Top1
            context.write(cs, scores.iterator().next());
        }
    }
}
