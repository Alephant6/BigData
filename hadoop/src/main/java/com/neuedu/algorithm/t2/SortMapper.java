package com.neuedu.algorithm.t2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 算法——排序值升序：自定义Mapper
 * @author Alephant
 */
public class SortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        // 源数据格式：1688
        String line = value.toString();
        // 为空处理
        if (StringUtils.isEmpty(line)) {
            return;
        }
        // 输出
        context.write(new IntWritable(Integer.valueOf(line)), NullWritable.get());
    }
}
