package com.neuedu.wordcount_order;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 自定义Mapper类：负责分，将一行文本拆分成N个单词
 *
 * @author Alephant
 */
public class WordCountMapper extends Mapper<LongWritable, Text, WordCount, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WordCount, NullWritable>.Context context) throws IOException, InterruptedException {
        // 将Text转换为String
        String line = value.toString();
        // 数据清洗：不能为空
        if (StringUtils.isBlank(line)) {
            return;
        }
        // 分
        // 简单的单词拆分工具类
        StringTokenizer st = new StringTokenizer(line);
        while (st.hasMoreTokens()) {
            // 获取单词
            String word = st.nextToken();
            // 输出结果
            WordCount wc = new WordCount(word, 1);
            context.write(wc, NullWritable.get());
        }
    }
}
