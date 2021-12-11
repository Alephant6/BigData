package com.neuedu.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 编码5：自定义分组GroupByYear
 * 功能
 * 按照年份进行分组
 *
 * @author zhang
 */
public class GroupByYear extends WritableComparator {
    public GroupByYear() {
        super(WeatherWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 因为是分组，所以只比较年份，且仅关心等于0的情况，等于0是同一组
        WeatherWritable w1 = (WeatherWritable) a;
        WeatherWritable w2 = (WeatherWritable) b;
        return Integer.compare(w1.getYear(), w2.getYear());
    }
}
