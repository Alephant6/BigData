package com.neuedu.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 编码4：定义排序类
 * @author Alephant
 * 功能：
 *     reduce端
 *     温度从高到底排列，去第一个就是最高温度
 *     年份不同，比较年份，升序
 *     年份相同，比较温度，降序
 */
public class SortedBYHotDESC extends WritableComparator {
    public SortedBYHotDESC() {
        super(WritableComparable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherWritable w1 = (WeatherWritable) a;
        WeatherWritable w2 = (WeatherWritable) b;
        // 年份不同时，比较年份，升序
        if (w1.getYear() != w2.getYear()){
            return Integer.compare(w1.getYear(), w2.getYear());
        }
        return -Integer.compare((w1.getHot()), w2.getHot());
    }
}
