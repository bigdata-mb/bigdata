package com.mamba.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 这个类作为mr程序的入口类，这里面写main方法
  */
public class WordCount extends Configured implements Tool{
    /**
     * 实现Tool接口之后，需要实现一个run方法，
     * 这个run方法用于组装我们的程序的逻辑，其实就是组装八个步骤
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        //获取Job对象，组装我们的八个步骤，每一个步骤都是一个class类
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "mrdemo");

        //实际工作当中，程序运行完成之后一般都是打包到集群上面去运行，打成一个jar包
        //如果要打包到集群上面去运行，必须添加以下设置
        job.setJarByClass(WordCount.class);

        //第一步：读取文件，解析成key,value对，k1:行偏移量 v1：一行文本内容
        job.setInputFormatClass(TextInputFormat.class);
        //指定我们去哪一个路径读取文件
        TextInputFormat.addInputPath(job,new Path("file:///D:\\test"));
        //第二步：自定义map逻辑，接受k1   v1 转换成为新的k2   v2输出
        job.setMapperClass(MyMapper.class);
        //设置map阶段输出的key,value的类型，其实就是k2 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //第三步到六步：分区，排序，规约，分组都省略
        //第七步：自定义reduce逻辑
        job.setReducerClass(MyReducer.class);
        //设置key3 value3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //第八步：输出k3 v3 进行保存
        job.setOutputFormatClass(TextOutputFormat.class);
        //一定要注意，输出路径是需要不存在的，如果存在就报错
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\test\\out_result"));
        //提交job任务
        boolean b = job.waitForCompletion(true);
        return b?0:1;
        /***
         * 第一步：读取文件，解析成key,value对，k1   v1
         * 第二步：自定义map逻辑，接受k1   v1 转换成为新的k2   v2输出
         * 第三步：分区。相同key的数据发送到同一个reduce里面去，key合并，value形成一个集合
         * 第四步：排序   对key2进行排序。字典顺序排序
         * 第五步：规约 combiner过程 调优步骤 可选
         * 第六步：分组
         * 第七步：自定义reduce逻辑接受k2   v2 转换成为新的k3   v3输出
         * 第八步：输出k3 v3 进行保存
         */
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("hello","world");
        //提交run方法之后，得到一个程序的退出状态码
        int run = ToolRunner.run(configuration, new WordCount(), args);
        //根据我们 程序的退出状态码，退出整个进程
        System.exit(run);
    }
}

