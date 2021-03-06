package com.yh_simon.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//记得 打包成 jar包
public class JobMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","root");
        //启动一个任务
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new JobMain(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        //创建一个任务对象  从父类中 获取conf
        Job job = Job.getInstance(super.getConf(), "mapreudce-wordcount");

        //打包放在集群运行时，需要做一个配置
        job.setJarByClass(JobMain.class);

        //第一步  设置读取文件的类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/wordcount"));

        //第二步  设置Mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置Map阶段的输出类型 : k2 和 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //第三、四、五、六步采用默认方式(分区、排序、规约、分组)  shuffle阶段
        //设置自定义规约
        job.setCombinerClass(MyCombiner.class);

        //第七步  设置Reducer类
        job.setReducerClass(WordCountReducer.class);
        //设置Reducer的个数
        //job.setNumReduceTasks(2);

        //设置Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //第八步  设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径  自动创建该路径  若已存在，需先删除
        TextOutputFormat.setOutputPath(job, new Path("hdfs://node01:8020/wordcount_out"));

        //等待完成状态
        boolean b = job.waitForCompletion(true);

        //true 0  : false 1
        return b?0:1;
    }
}
