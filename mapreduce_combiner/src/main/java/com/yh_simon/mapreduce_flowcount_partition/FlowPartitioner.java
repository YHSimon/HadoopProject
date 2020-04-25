package com.yh_simon.mapreduce_flowcount_partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //判断手机号 开头数
        if (text.toString().startsWith("135")) {
            return 0;
        } else if (text.toString().startsWith("136")) {
            return 1;
        } else if (text.toString().startsWith("138")) {
            return 2;
        } else if (text.toString().startsWith("139")) {
            return 3;
        } else {
            return 4;
        }
    }
}
