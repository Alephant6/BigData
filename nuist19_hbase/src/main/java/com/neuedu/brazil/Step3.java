package com.neuedu.brazil;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 统计汇总每年的最高气温
 *
 * @author Alephant
 */
public class Step3 {
    private static class Step3Mapper extends TableMapper<Text, FloatWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            // 读取行键
            // code_date-->83377_31/12/2019
            String rowKey = Bytes.toString(value.getRow());
            // 拆分得到code和year
            String code = rowKey.split("_")[0];
            String year = rowKey.split("_")[1].substring(6);
            // 读取一行数据，遍历每1列
            for (Cell cell : value.rawCells()) {
                // 读取列名
                String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                if ("maxtemperature".equals(c)) {
                    // 读取最高温度
                    Float maxtemperature = Bytes.toFloat(CellUtil.cloneValue(cell));
                    // 输出
                    context.write(new Text(code + "_" + year), new FloatWritable(maxtemperature));
                }
            }
        }
    }

    private static class Step3Reducer extends TableReducer<Text, FloatWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            // 接收的数据：<83377_2019,<32,28,66...>>
            Float maxtemperature = 0.0f;
            // 遍历所有最高温度，求出最高温度
            for (FloatWritable v : values) {
                maxtemperature = Math.max(maxtemperature, v.get());
            }
            // 定义列簇、列、值
            byte[] family = Bytes.toBytes("info");
            byte[] c = Bytes.toBytes("maxtemperature");
            byte[] v = Bytes.toBytes(maxtemperature);
            // 定义行键
            byte[] rowKey = Bytes.toBytes(key.toString());
            // 构建Put对象
            Put put = new Put(rowKey);
            put.addColumn(family, c, v);
            // 输出
            context.write(NullWritable.get(), put);
        }
    }


    /**
     * 显示表results所有数据
     */
    private static void showResults() {
        try {
            // 构建TableName对象
            TableName tn = TableName.valueOf("results");
            // 连接对象
            Connection connection = HbaseUtils.getCONNECTION();
            // 获取表对象
            Table results = connection.getTable(tn);
            // 定义Scan全表扫描对象
            Scan scan = new Scan();
            // 读取表中所有数据
            ResultScanner rows = results.getScanner(scan);
            // 打印表头
            System.out.println("气象站编号\t年份\t\t降雨天数\t\t最高温度\t\t最低温度\t\t平均温度");
            // 定义变量
            String code = "";
            String year = "";
            Float pecipitation = 0.0F;
            Float maxtemperature = 0.0F;
            Float mintemperature = 0.0F;
            Float avgtemperature = 0.0F;
            // 遍历所有行
            for (Result r : rows) {
                // 读取行键
                String rowKey = Bytes.toString(r.getRow());
                code = rowKey.split("_")[0];
                year = rowKey.split("_")[1];
                for (Cell cell : r.rawCells()) {
                    // 读取列名
                    String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                    switch (c) {
                        case "pecipitation":
                            pecipitation = Bytes.toFloat(CellUtil.cloneValue(cell));
                            break;
                        case "maxtemperature":
                            maxtemperature = Bytes.toFloat(CellUtil.cloneValue(cell));
                            break;
                        case "mintemperature":
                            mintemperature = Bytes.toFloat(CellUtil.cloneValue(cell));
                            break;
                        case "avgtemperature":
                            avgtemperature = Bytes.toFloat(CellUtil.cloneValue(cell));
                            break;
                    }
                }
                System.out.println(code + "\t\t" + year + "\t\t" + pecipitation + "\t\t" + maxtemperature +
                        "\t\t" + mintemperature + "\t\t" + avgtemperature);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            // 定义输出表名
            String tableName = "results";
            // 创建输出表：存在，不删除
            HbaseUtils.createTable(tableName, "info", false);
            // 构建Job对象
            Job job = Job.getInstance(HbaseUtils.getCONF(), "max temperature of year");
            // 定义输入表名、列簇、列
            String weather = "weather";
            byte[] family = Bytes.toBytes("info");
            byte[] c = Bytes.toBytes("maxtemperature");
            // 构建Scan对象
            Scan scan = new Scan();
            scan.addColumn(family, c);
            // 设置mapper
            TableMapReduceUtil.initTableMapperJob(weather, scan, Step3Mapper.class,
                    Text.class, FloatWritable.class, job);
            // 设置reducer
            TableMapReduceUtil.initTableReducerJob(tableName, Step3Reducer.class, job);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                // 显示结果
                showResults();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
