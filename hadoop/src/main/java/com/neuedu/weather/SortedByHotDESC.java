package com.neuedu.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 编码4：自定义排序
 * 功能
 * reduce端
 * 温度从高到低排列，取第一个，不就是最高温度
 * 年份不同，比较年份，升序
 * 年份相同，比较温度，降序
 *
 * @author Alephant
 */
public class SortedByHotDESC extends WritableComparator {
    public SortedByHotDESC() {
        super(WeatherWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherWritable w1 = (WeatherWritable) a;
        WeatherWritable w2 = (WeatherWritable) b;
        // 年份不同时，比较年份，升序
        if (w1.getYear() != w2.getYear()) {
            return Integer.compare(w1.getYear(), w2.getYear());
        }
        // 年份相同时，比较温度，降序
        return -Integer.compare(w1.getHot(), w2.getHot());
    }
}
