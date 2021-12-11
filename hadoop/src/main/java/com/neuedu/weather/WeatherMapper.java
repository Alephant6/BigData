package com.neuedu.weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 编码2：自定义Mapper类WeatherMapper
 * <p>
 * 数据清洗：
 * null或空字符串，不符合
 * 拆分结果不正确
 * 功能：
 * 将行解析成一个实体类
 * 输出:
 * key就是实体类
 * value其实是可以为空，此处暂时为整行文本，最终为null
 *
 * @author Alephant
 */
public class WeatherMapper extends Mapper<LongWritable, Text, WeatherWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WeatherWritable, Text>.Context context) throws IOException, InterruptedException {
        // 1949-10-01 14:21:02 34℃
        // 1949-10-01 14:21:02 4℃
        // 数据清洗
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }
        String[] items = line.split("\t");
        if (items.length != 2) {
            return;
        }
        // 转换成实体类WeatherWritable
        int year = Integer.parseInt(items[0].substring(0, 4));
        int hot = Integer.parseInt(items[1].substring(0, items[1].length() - 1));
        WeatherWritable w = new WeatherWritable(year, hot);
        // 输出
        context.write(w, value);
    }
}
