package my.mapreduce02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class FlowExercise1 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowExercise1.class);

        job.setMapperClass(FlowExercise1Mapper.class);
        job.setReducerClass(FlowExercise1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, "d:/flow/input");
        FileOutputFormat.setOutputPath(job, new Path("d:/flow/output13"));

        boolean status = job.waitForCompletion(true);
        System.exit(status? 0 : 1);
    }

    static class FlowExercise1Mapper extends Mapper<LongWritable, Text, Text, Flow> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");

            String phone = splits[1];
            long upflow = Long.parseLong(splits[8]);
            long downflow = Long.parseLong(splits[9]);

            Flow flow = new Flow(upflow, downflow);
            context.write(new Text(phone), flow);
        }
    }

    static class FlowExercise1Reducer extends Reducer<Text, Flow, Text, Flow> {
        protected void reduce(Text phone, Iterable<Flow> flows, Mapper.Context context)
                throws IOException, InterruptedException {

            long sumUpflow = 0;    // 该phone用户的总上行流量
            long sumDownflow = 0;
            for(Flow f : flows){
                sumUpflow += f.getUpflow();
                sumDownflow += f.getDownflow();
            }
            Flow sumFlow = new Flow(sumUpflow, sumDownflow);
            context.write(phone, sumFlow);

//          String v = sumUpflow +"\t" + sumDownflow +"\t" + (sumUpflow + sumDownflow);
//          context.write(phone, new Text(v));
        }
    }
}
