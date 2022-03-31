package com.neuedu.wordcount;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Reducer，负责词频统计，并保证统计信息写入hbase表中
 *
 * @author Alephant
 */
public class WordCountReducer extends TableReducer<Text, IntWritable, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
        // 统计词频信息
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        // 通过Put操作将词频信息写入hbase表中
        byte[] rowkey = Bytes.toBytes(key.toString());
        Put put = new Put(rowkey);
        // 定义列簇，列，值
        byte[] f = Bytes.toBytes("info");
        byte[] c = Bytes.toBytes("count");
        byte[] v = Bytes.toBytes(sum);
        put.addColumn(f, c, v);
        // 写入hbase表中
        context.write(NullWritable.get(), put);
    }
}
