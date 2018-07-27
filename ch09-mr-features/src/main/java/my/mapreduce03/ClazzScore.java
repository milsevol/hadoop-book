package my.mapreduce03;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
/**
 * Created by xueyong.cui on 2018/7/26.
 */
public class ClazzScore implements WritableComparable<ClazzScore>{
    private String clazz;
    private Double score;
    public String getClazz() {
        return clazz;
    }
    public void setClazz(String clazz) {
        this.clazz = clazz;
    }
    public Double getScore() {
        return score;
    }
    public void setScore(Double score) {
        this.score = score;
    }
    public ClazzScore(String clazz, Double score) {
        super();
        this.clazz = clazz;
        this.score = score;
    }
    public ClazzScore() {
        super();
// TODO Auto-generated constructor stub
    }
    @Override
    public String toString() {
        return clazz + "\t" + score;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(clazz);
        out.writeDouble(score);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
// TODO Auto-generated method stub
        this.clazz = in.readUTF();
        this.score = in.readDouble();
    }
    /**
     * key 排序
     */
    @Override
    public int compareTo(ClazzScore cs) {
        int it = cs.getClazz().compareTo(this.clazz);
        if(it == 0){
            return (int) (cs.getScore() - this.score);
        }else{
            return it;
        }
    }

}
