package my.mapreduce02;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by xueyong.cui on 2018/7/26.
 */
public class FlowExercise2Sort {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowExercise2Sort.class);

        job.setMapperClass(FlowExercise2SortMapper.class);
        job.setReducerClass(FlowExercise2SortReducer.class);

        job.setMapOutputKeyClass(Flow.class);
        job.setMapOutputValueClass(Text.class);

//      job.setCombinerClass(FlowExercise1Combiner.class);
//      job.setCombinerClass(FlowExercise1Reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Flow.class);

        FileInputFormat.setInputPaths(job, "d:/flow/output1");
        FileOutputFormat.setOutputPath(job, new Path("d:/flow/sortoutput6"));

        boolean status = job.waitForCompletion(true);
        System.exit(status? 0 : 1);
    }

    static class FlowExercise2SortMapper extends Mapper<LongWritable, Text, Flow, Text>{
        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, Flow, Text>.Context context)
                throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");

            String phone = splits[0];
            long upflow = Long.parseLong(splits[1]);
            long downflow = Long.parseLong(splits[2]);
//          long sumflow = Long.parseLong(splits[3]);
            Flow flow = new Flow(upflow, downflow, phone);

            context.write(flow, new Text(phone));
        }
    }

    static class FlowExercise2SortReducer extends Reducer<Flow, Text, NullWritable, Flow>{
        @Override
        protected void reduce(Flow flow, Iterable<Text> phones, Context context)
                throws IOException, InterruptedException {

            for(Text t : phones){
                context.write(NullWritable.get(), flow);
            }
        }
    }

}
