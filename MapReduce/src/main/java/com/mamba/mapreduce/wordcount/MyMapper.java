package com.mamba.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义mapper类需要继承Mapper，有四个泛型，
 * keyin: k1   行偏移量 Long
 * valuein: v1   一行文本内容   String
 * keyout: k2   每一个单词   String
 * valueout : v2   1         int
 * 在hadoop当中没有沿用Java的一些基本类型，使用自己封装了一套基本类型
 * long ==>LongWritable
 * String ==> Text
 * int ==> IntWritable
 *
 */
public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    /**
     * 继承mapper之后，覆写map方法，每次读取一行数据，都会来调用一下map方法
     * @param key：对应k1
     * @param value:对应v1
     * @param context 上下文对象。承上启下，承接上面步骤发过来的数据，通过context将数据发送到下面的步骤里面去
     * @throws IOException
     * @throws InterruptedException
     * k1   v1
     * 0；hello,world
     *
     * k2 v2
     * hello 1
     * world   1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取我们的一行数据
        String line = value.toString();
        String[] split = line.split(",");
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);
        for (String word : split) {
            //将每个单词出现都记做1次
            //key2 Text类型
            //v2 IntWritable类型
            text.set(word);
            //将我们的key2 v2写出去到下游
            context.write(text,intWritable);
        }
    }
}
