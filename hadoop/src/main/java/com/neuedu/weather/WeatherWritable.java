package com.neuedu.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 编码1：自定义实体类WeatherWritable
 * @author Alephant
 */
public class WeatherWritable implements WritableComparable<WeatherWritable> {
    /**
     * 年份
     */
    private int year;
    /**
     * 最高温度
     */
    private int hot;

    /**
     * 构造器
     */
    public WeatherWritable() {
    }

    /**
     * 构造器
     * @param year 年份
     * @param hot 最高温度
     */
    public WeatherWritable(int year, int hot) {
        this.year = year;
        this.hot = hot;
    }

    @Override
    public String toString() {
        return this.year + "\t" + this.hot;
    }

    @Override
    public int compareTo(WeatherWritable other) {
        // 默认比较：通常都是升序
        // other为null
        if (other == null) {
            return 1;
        }
        // 年份不同
        if (this.year != other.year) {
            return Integer.compare(this.year, other.year);
        }
        // 年份相同
        return Integer.compare(this.hot,other.hot);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 序列化
        out.writeInt(this.year);
        out.writeInt(this.hot);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 反序列化
        this.year = in.readInt();
        this.hot = in.readInt();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }
}
