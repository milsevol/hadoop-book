package my.mapreduce04;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by 000 on 2018/7/26.
 */
public class MovieRatingMapJoinMR {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        System.setProperty("HADOOP_USER_NAME","hadoop");
        Job job = Job.getInstance(conf);
// job.setJarByClass(MovieRatingMapJoinMR.class);
    //    job.setJar("/home/hadoop/mrmr.jar");
        job.setMapperClass(MovieRatingMapJoinMRMapper.class);
        job.setMapOutputKeyClass(MovieRate.class);
        job.setMapOutputValueClass(NullWritable.class);
// job.setReducerClass(MovieRatingMapJoinMReducer.class);
// job.setOutputKeyClass(MovieRate.class);
// job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        String minInput = args[0];
        String maxInput = args[1];
        String output = args[2];
        FileInputFormat.setInputPaths(job, new Path(maxInput));
        Path outputPath = new Path(output);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        URI uri = new Path(minInput).toUri();
     //   job.addCacheFile(uri);
        boolean status = job.waitForCompletion(true);
        System.exit(status?0:1);
    }

    private static class MovieRatingMapJoinMRMapper extends Mapper<LongWritable,Text,MovieRate,NullWritable>{
        // 用来存储小份数据的所有解析出来的key-value
    //    private static Map<String,Movie> movieMap = new HashMap<String, Movie>();

    }
}
