package com.yh_simon.mapreduce_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text,PairWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //自定义计数器
        context.getCounter("MR_COUNTERS", "MapCounter").increment(1L);

        //1.对每一行数据拆分，封装到PairWritable对象  作为k2
        String[] split = value.toString().split("\t");
        PairWritable pairWritable = new PairWritable();
        pairWritable.setFirst(split[0]);
        pairWritable.setSecond(Integer.parseInt(split[1]));

        //2.将k2 v2写入上下文中
        context.write(pairWritable, value);
    }

}
