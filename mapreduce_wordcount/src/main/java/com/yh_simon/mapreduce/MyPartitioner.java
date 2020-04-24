package com.yh_simon.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {

    /**
     * @param text         表示k2
     * @param longWritable 表示V2
     * @param i            reducer个数  假设分区只有两个
     * @return 返回分区的编号
     */
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {

        //假设单词长度>=5，进入第一个分区（编号为0）
        if (text.toString().length() >= 5) {
            return 0;
        } else {
            return 1;
        }
    }
}
