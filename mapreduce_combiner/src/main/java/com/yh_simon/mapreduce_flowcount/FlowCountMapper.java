package com.yh_simon.mapreduce_flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        //手机号
        String phoneNum = split[1];
        //获取四个流量字段
        FlowBean flowBean=new FlowBean();
        flowBean.setUpFlow(Long.parseLong(split[2]));
        flowBean.setDownFlow(Long.parseLong(split[3]));
        flowBean.setUpCountFlow(Long.parseLong(split[4]));
        flowBean.setDownCountFlow(Long.parseLong(split[5]));

        //写入上下文
        context.write(new Text(phoneNum), flowBean);
    }
}
