package com.yh_simon.mapreduce_reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper   extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //首先判断数据来自哪个文件
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if(fileName.equals("orders.txt")){
            //获取pid
            String[] split = value.toString().split(",");
            context.write(new Text(split[2]),value);
        }else{
            //获取pid
            String[] split = value.toString().split(",");
            context.write(new Text(split[0]),value);
        }
    }
}
