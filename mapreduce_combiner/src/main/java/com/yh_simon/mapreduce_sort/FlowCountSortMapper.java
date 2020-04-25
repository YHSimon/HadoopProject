package com.yh_simon.mapreduce_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        //设置 流量信息为k2   手机号为 v2
        String phoneNum = split[0];
        FlowBean flowBean = new FlowBean(Long.parseLong(split[1]), Long.parseLong(split[2]), Long.parseLong(split[3]), Long.parseLong(split[4]));
        context.write(flowBean, new Text(phoneNum));
    }
}
