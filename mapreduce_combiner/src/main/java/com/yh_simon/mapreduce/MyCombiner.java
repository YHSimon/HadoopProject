package com.yh_simon.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //遍历values集合
        long count = 0;
        for (LongWritable value : values) {
            //相加
            count += value.get();
        }
        //将k3 v3 写入上下文中
        context.write(key, new LongWritable(count));
    }
}
