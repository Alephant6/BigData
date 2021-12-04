package com.neuedu.wordcount_combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 自定义Reducer类；负责汇总，统计单词的次数，输出<单词，次数>
 *
 * @author Alephant
 */

public class WordCountReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 统计单词出现的总次数
        int sum = 0;
        for (IntWritable v : values){
            sum += v.get();
        }
        // 输出
        context.write(key, new IntWritable(sum));
    }
}
