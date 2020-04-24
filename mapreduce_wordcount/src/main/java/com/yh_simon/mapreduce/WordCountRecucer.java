package com.yh_simon.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/*
 * KEYIN     -->k2     每个单词                Text
 * VALUEIN,  -->v2     集合中泛型的类型        LongWritable
 * KEYOUT,   -->k3     每个单词                Text
 * VALUEOUT  -->v3     每个单词出现的次数      LongWritable
 */
public class WordCountRecucer extends Reducer<Text, LongWritable,Text ,LongWritable > {

    /**
     * 重写reduce方法
     * 作用：将<k2,v2>   -->   <k3,v3>
     *
     *
        k2          v2
       hello       <1,1>
       world       <1,1,1>


        k3          v3
      hello          2
      world          3


     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //遍历values集合
        long count=0;
        for(LongWritable value :values){
            //相加
            count+=value.get();
        }
        //将k3 v3 写入上下文中
        context.write(key, new LongWritable(count));
    }
}
