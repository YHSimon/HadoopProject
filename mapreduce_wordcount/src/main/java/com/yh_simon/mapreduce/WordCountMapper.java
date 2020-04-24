package com.yh_simon.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * KEYIN  -->k1  行偏移量  LongWritable
 * VALUEIN,  -->v1  一行的文本数据  Text
 * KEYOUT,   -->k2     每个单词     Text
 * VALUEOUT  -->v2      固定值1     LongWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {


    /**
     * 重写map方法
     * 作用：将<k1,v1>  -->  <k2,v2>

     k1   v1
     0    hello,world
     11   hello,hadoop

     k2      v2
     hello    1
     world    1
     hello    1
     hadoop   1
     * @param key  k1
     * @param value  v1
     * @param context   表示MapReduce上下文对象  （实现map 与 reduce的连接）
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();

        //对每一行数据进行拆分
        String line=value.toString();
        String[] split = line.split(",");
        //遍历数组，获取每一个单词
        for (String word:split){
//            context.write(new Text(word), new LongWritable(1));
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}

