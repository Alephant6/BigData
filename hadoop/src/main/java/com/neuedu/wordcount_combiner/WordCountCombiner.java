package com.neuedu.wordcount_combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Combiner归并（相当于是本台服务器的map输出结果一个小计），
 *
 * @author Alephant
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 归并属于map阶段
        //数据就是本机的map输出
        // 统计相同单词出现的次数
        int sum = 0;
        for (IntWritable v : values){
            sum ++;
        }
        // 输出给reduce
        context.write(key, new IntWritable(sum));
    }
}
