package com.yh_simon.mapreduce_flowcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long upFlow=0L;
        Long downFlow=0L;
        Long upCountFlow=0L;
        Long downCountFlow=0L;
        for(FlowBean value:values){
            upFlow+=value.getUpFlow();
            downFlow+=value.getDownFlow();
            upCountFlow+=value.getUpCountFlow();
            downCountFlow+=value.getDownCountFlow();
        }
        FlowBean flowBean = new FlowBean(upFlow, downFlow, upCountFlow, downCountFlow);
        //将k3 v3 写入上下文
        context.write(key, flowBean);
    }
}
