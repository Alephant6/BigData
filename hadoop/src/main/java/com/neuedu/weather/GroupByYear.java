package com.neuedu.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 编程5
 * @author Alephant
 */
public class GroupByYear extends WritableComparator {
    public GroupByYear() {
        super(WeatherWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 因为是分组，说以只比较年份，且仅关心
        WeatherWritable w1 = (WeatherWritable) a;
        WeatherWritable w2 = (WeatherWritable) b;
        return Integer.compare((w1.getHot()), w2.getHot());
    }
}
