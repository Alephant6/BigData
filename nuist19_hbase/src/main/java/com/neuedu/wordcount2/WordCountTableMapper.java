package com.neuedu.wordcount2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 自定义TableMapper，负责从hbase表中读取数据，并进行数据拆分
 *
 * @author Alephant
 */
public class WordCountTableMapper extends TableMapper<Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 获取行键值
        // String rowkey = Bytes.toString(value.getRow());
        // 读取行的内容
        for (Cell cell : value.rawCells()) {
            // 获取行键值
            //String r = Bytes.toString(CellUtil.cloneRow(cell));
            // 其它业务需求：读取列族名称
            //String f = Bytes.toString(CellUtil.cloneFamily(cell));
            // 其它业务需求：读取列名
            //String c = Bytes.toString(CellUtil.cloneQualifier(cell));
            // 获取单元格内容
            String line = Bytes.toString(CellUtil.cloneValue(cell));
            // 拆分单词
            StringTokenizer st = new StringTokenizer(line);
            while (st.hasMoreTokens()) {
                // 读取单词
                String word = st.nextToken();
                // 输出
                System.out.println(word);
                context.write(new Text(word), ONE);
            }
        }
    }
}
