package com.neuedu.weather;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 编码3：自定义分区类WeatherPartitioner
 * 功能
 * 需求：每年最高温度
 * 分区就是将同一年的数据放在一起，交给一个reduce处理，让它找出最高温度
 *
 * @author Alephant
 */
public class WeatherPartitioner extends Partitioner<WeatherWritable, NullWritable> {
    @Override
    public int getPartition(WeatherWritable key, NullWritable value, int numPartitions) {
        // 特别强调：算法不能太复杂，越简单越好
        // 寻找规律
        // 1949 -> 1949-1940 = 9 -> 9 % 3 = 0
        // 1950 -> 1950-1940 = 10-> 10% 3 = 1
        // 1951 -> 1951-1940 = 11-> 11% 3 = 2
        // 同学问:100年，1900-1900=0,1901-1900=1,...,1949-1900=49,1950-1900=50,1951-1900=51,...,1999-1900=99
        return (key.getYear() - 1940) % numPartitions;
    }
}
