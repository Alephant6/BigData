package com.neuedu.wordcount_partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 自定义Mapper类，负责分，江一行文本分成N个单词，输出<单词，1>
 *
 * @author Alephant
 */


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 将Text转换为String
        String line = value.toString();
        // 数据清洗：不能为空
        if (StringUtils.isBlank(line)) {
            return;
        }
        // 分
        // 简单的单词拆分工具类
        StringTokenizer st = new StringTokenizer(line);
        while (st.hasMoreElements()) {
            // 获取单词
            String word = st.nextToken();
            // 输出结果
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
