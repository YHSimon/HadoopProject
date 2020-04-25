package com.yh_simon.mapreduce_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * k2  流量信息
 * v2   手机号
 * k3   手机号
 * v3   流量信息
 */
public class FlowCountSortReducer extends Reducer<FlowBean, Text,Text,FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value:values){
            context.write(value, key);
        }
    }
}
