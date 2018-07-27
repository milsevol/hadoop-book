package my.mapreduce04;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 * Created by xueyong.cui on 2018/7/26.
 */
public class MyScoreOutputFormat  extends TextOutputFormat<Text, NullWritable>{
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(
            TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration configuration = job.getConfiguration();

        FileSystem fs = FileSystem.get(configuration);
        Path p1 = new Path("/score1/outpu1");
        Path p2 = new Path("/score2/outpu2");

        if(fs.exists(p1)){
            fs.delete(p1, true);
        }
        if(fs.exists(p2)){
            fs.delete(p2, true);
        }

        FSDataOutputStream fsdout1 = fs.create(p1);
        FSDataOutputStream fsdout2 = fs.create(p2);
        return new MyRecordWriter(fsdout1, fsdout2);
    }

    static class MyRecordWriter extends RecordWriter<Text, NullWritable>{

        FSDataOutputStream dout1 = null;
        FSDataOutputStream dout2 = null;

        public MyRecordWriter(FSDataOutputStream dout1, FSDataOutputStream dout2) {
            super();
            this.dout1 = dout1;
            this.dout2 = dout2;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub

            String[] strs = key.toString().split("::");
            if(strs[0].equals("1")){
                dout1.writeBytes(strs[1]+"\n");
            }else{
                dout2.writeBytes(strs[1]+"\n");
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            IOUtils.closeStream(dout2);
            IOUtils.closeStream(dout1);
        }
    }
}
