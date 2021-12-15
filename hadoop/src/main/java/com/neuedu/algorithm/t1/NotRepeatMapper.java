package com.neuedu.algorithm.t1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 算法--数据去重：自定义Mapper
 * @author Alephant
 */
public class NotRepeatMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 源数据格式：2012-3-1 a
        String line = value.toString();
        // 为空处理
        if (StringUtils.isEmpty(line)) {
            return;
        }
        // 输出
        context.write(value, NullWritable.get());
    }
}
