package my.mapreduce03;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by xueyong.cui on 2018/7/26.
 */
public class ClazzScoreGroupComparator extends WritableComparator {

    ClazzScoreGroupComparator(){
        super(ClazzScore.class, true);
    }
    /**
     * 决定输入到 reduce 的数据的分组规则
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
// TODO Auto-generated method stub
        ClazzScore cs1 = (ClazzScore)a;
        ClazzScore cs2 = (ClazzScore)b;
        int it = cs1.getClazz().compareTo(cs2.getClazz());
        return it;
    }
}
