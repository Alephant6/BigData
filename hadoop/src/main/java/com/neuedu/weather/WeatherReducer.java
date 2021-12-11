package com.neuedu.weather;

import org.apache.hadoop.io.NullWritable;
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
public class WeatherReducer extends Reducer<WeatherWritable, NullWritable,WeatherWritable, NullWritable> {
    @Override
    protected void reduce(WeatherWritable key, Iterable<NullWritable> values, Reducer<WeatherWritable, NullWritable, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        for (NullWritable v : values) {
            context.write(key, NullWritable.get());
            break;
        }
    }
}
