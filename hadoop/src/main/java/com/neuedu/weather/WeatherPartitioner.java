package com.neuedu.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 编码3：自定义分区
 * 需求：每年最高温度
 * 分区就是将同一年的数据放在一起，交给一个reduce来处理
 * @author Alephant
 */
public class WeatherPartitioner extends Partitioner<WeatherWritable, Text> {

    @Override
    public int getPartition(WeatherWritable key, Text value, int numPartitions) {
        // 算法不能太复杂，越简单越好
        // 1949 -> 1949 - 1940 = 9 -> 9 % 3 = 0
        // 1950 -> 1950 - 1940 = 10 -> 10 % 3 = 1
        // 1951 -> 1951 - 1940 = 11 -> 11 % 3 = 2
        // 100年，1900-1900=0,...,1949-1900=49,...,1999-1900=99
        return (key.getYear() - 1940) % numPartitions;
    }
}
