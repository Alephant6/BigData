package com.neuedu.wordcount_oop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer <WordCount, NullWritable, WordCount, NullWritable> {
    @Override
    protected void reduce(WordCount key, Iterable<NullWritable> values, Reducer<WordCount, NullWritable, WordCount, NullWritable>.Context context) throws IOException, InterruptedException {
        // 统计单词出现的总次数
        int sum = 0;
        for (NullWritable v : values){
            sum += 1;
        }
        // 输出
        key.setCount(sum);
        context.write(key, NullWritable.get());
    }
}
