package com.yh_simon.mapreduce_sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //创建一个任务对象  从父类中 获取conf
        Job job = Job.getInstance(super.getConf(), "mapreudce-wordcount");

        //打包放在集群运行时，需要做一个配置
        job.setJarByClass(com.yh_simon.mapreduce_sort.JobMain.class);

        //第一步  设置读取文件的类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/input/sort"));

        //第二步  设置Mapper类
        job.setMapperClass(SortMapper.class);
        //设置Map阶段的输出类型 : k2 和 v2的类型
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(Text.class);

        //第三、四、五、六步采用默认方式(分区、排序、规约、分组)  shuffle阶段

        //第七步  设置Reducer类
        job.setReducerClass(SortReducer.class);
        //设置Reducer的个数

        //设置Reducer输出类型
        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(NullWritable.class);

        //第八步  设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径  自动创建该路径  若已存在，需先删除
        TextOutputFormat.setOutputPath(job, new Path("hdfs://node01:8020/out/sort_out"));

        //等待完成状态
        boolean b = job.waitForCompletion(true);

        //true 0  : false 1
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
