package com.neuedu.bulkload;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义Mapper类BulkLoadMapper：读取HDFS中文件/emp/emp.txt，并解析
 *
 * @author alephant
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
        // 源数据格式
        // 1 zhangsan 22 neusoft
        String line = value.toString();
        // 数据清洗
        // 判断是否为null或空字符串
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] items = line.split(" ");
        // 判断是否符合特定格式
        if (items.length != 4) {
            return;
        }
        // 构造行键、列簇、列、值
        byte[] rowkey = Bytes.toBytes(items[0]);
        byte[] f = Bytes.toBytes("info");
        byte[] c1 = Bytes.toBytes("name");
        byte[] v1 = Bytes.toBytes(items[1]);
        byte[] c2 = Bytes.toBytes("age");
        byte[] v2 = Bytes.toBytes(items[2]);
        byte[] c3 = Bytes.toBytes("company");
        byte[] v3 = Bytes.toBytes(items[3]);
        // 构建Put对象
        Put put = new Put(rowkey);
        put.addColumn(f, c1, v1);
        put.addColumn(f, c2, v2);
        put.addColumn(f, c3, v3);
        // 输出
        context.write(new ImmutableBytesWritable(rowkey), put);
    }
}
