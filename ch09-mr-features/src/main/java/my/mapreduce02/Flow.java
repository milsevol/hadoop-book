package my.mapreduce02;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xueyong.cui on 2018/7/26.
 */
public class Flow implements WritableComparable<Flow> {
    private String phone;
    private long upflow;    // 上行流量
    private long downflow;  // 下行流量
    private long sumflow;   // 上行和下行流量之和

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    public Flow() {
    }
    public Flow(long upflow, long downflow) {
        super();
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow + downflow;
    }
    public Flow(long upflow, long downflow, String phone) {
        super();
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumflow = upflow + downflow;
        this.phone = phone;
    }
    @Override
    public String toString() {
        return phone +"\t" + upflow +"\t" + downflow +"\t" + sumflow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeLong(upflow);
        out.writeLong(downflow);
        out.writeLong(sumflow);
        out.writeUTF(phone);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.upflow = in.readLong();
        this.downflow = in.readLong();
        this.sumflow = in.readLong();
        this.phone = in.readUTF();
    }
    @Override
    public int compareTo(Flow flow) {
        if((flow.getSumflow() - this.sumflow) == 0){
            return this.phone.compareTo(flow.getPhone());
        }else{
            return (int)(flow.getSumflow() - this.sumflow);
        }
    }
}
