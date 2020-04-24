package com.yh_simon.mapreduce_sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//NullWritable 占位符 无意义
public class SortReducer extends Reducer<PairWritable, Text, PairWritable, NullWritable> {

    //自定义计数器(枚举方式)
    public enum MY_MAPREDUCE_COUNTERS{
        REDUCE_INPUT_KEY_COUNTER,REDUCE_INPUT_VALUE_COUNTER;
    }


    /*
        如果有两个相同的
        a   1   <a 1 ,a 1>
     */

    @Override
    protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //设置自定义计数器
        context.getCounter(MY_MAPREDUCE_COUNTERS.REDUCE_INPUT_KEY_COUNTER).increment(1L);
        for (Text value : values) {
            context.getCounter(MY_MAPREDUCE_COUNTERS.REDUCE_INPUT_VALUE_COUNTER).increment(1L);
            context.write(key, NullWritable.get());
        }
    }
}
