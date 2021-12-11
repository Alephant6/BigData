package com.neuedu.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 编码6：自定义Redcuer类WeatherReducer
 * 功能
 * 接收到的数据是按年份分组的
 * 年份从低到高依次排列
 * 年份相同时，接收到的数据是按温度从高到低排序的
 *
 * @author Alephant
 */
public class WeatherReducer extends Reducer<WeatherWritable, Text,WeatherWritable, Text> {
    @Override
    protected void reduce(WeatherWritable key, Iterable<Text> values, Reducer<WeatherWritable, Text, WeatherWritable, Text>.Context context) throws IOException, InterruptedException {
        for (Text v : values) {
            context.write(key, v);
        }
    }
}
