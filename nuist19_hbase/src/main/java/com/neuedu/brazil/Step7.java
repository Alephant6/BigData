package com.neuedu.brazil;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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
import java.util.Scanner;

/**
 * 预测指定日期的天气
 *
 * @author Alephant
 */
public class Step7 {
    private static class Step7Mapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            // 读取行键
            // code_date-->83377_31/12/2019
            String rowKey = Bytes.toString(value.getRow());
            // 拆分得到code和year
            String code = rowKey.split("_")[0];
            // 读取预测日期
            String date = context.getConfiguration().get("date");
            // 筛选过滤历史上的与预测日期相同的天气数据
            if (!rowKey.split("_")[1].startsWith(date)) {
                return;
            }
            // 定义变量读取数据
            Float pecipitation = 0.0f;
            Float maxtemperature = 0.0f;
            Float mintemperature = 0.0f;
            Float avgtemperature = 0.0f;
            // 读取一行数据，遍历每1列
            for (Cell cell : value.rawCells()) {
                // 读取列名
                String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                switch (c) {
                    case "pecipitation":
                        // 读取降雨量
                        pecipitation = Bytes.toFloat(CellUtil.cloneValue(cell));
                        break;
                    case "maxtemperature":
                        // 读取最高温度
                        maxtemperature = Bytes.toFloat(CellUtil.cloneValue(cell));
                        break;
                    case "mintemperature":
                        // 读取最低温度
                        mintemperature = Bytes.toFloat(CellUtil.cloneValue(cell));
                        break;
                    case "avgtemperature":
                        // 读取平均温度
                        avgtemperature = Bytes.toFloat(CellUtil.cloneValue(cell));
                        break;
                }
            }
            // 输出
            context.write(new Text(code + "_" + date), new Text(pecipitation
                    + "_" + maxtemperature + "_" + mintemperature + "_" + avgtemperature));
        }
    }

    private static class Step7Reducer extends TableReducer<Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            // 接收的数据：<83377_21/04,<18_34_22_25,18_34_22_25...>>
            // 定义变量
            Float pecipitation = 0.0f;
            Float maxtemperature = 0.0f;
            Float mintemperature = 0.0f;
            Float avgtemperature = 0.0f;
            int count = 0;
            // 遍历历史上的今天
            for (Text v : values) {
                String[] items = v.toString().split("_");
                pecipitation += Float.parseFloat(items[0]);
                maxtemperature += Float.parseFloat(items[1]);
                mintemperature += Float.parseFloat(items[2]);
                avgtemperature += Float.parseFloat(items[3]);
                //计数
                count++;
            }
            // 计算平均值
            pecipitation /= count;
            maxtemperature /= count;
            mintemperature /= count;
            avgtemperature /= count;
            // 定义列簇、列、值
            byte[] family = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("pecipitation");
            byte[] c2 = Bytes.toBytes("maxtemperature");
            byte[] c3 = Bytes.toBytes("mintemperature");
            byte[] c4 = Bytes.toBytes("avgtemperature");
            byte[] v1 = Bytes.toBytes(pecipitation);
            byte[] v2 = Bytes.toBytes(maxtemperature);
            byte[] v3 = Bytes.toBytes(mintemperature);
            byte[] v4 = Bytes.toBytes(avgtemperature);
            // 定义行键
            byte[] rowKey = Bytes.toBytes(key.toString());
            // 构建Put对象
            Put put = new Put(rowKey);
            put.addColumn(family, c1, v1);
            put.addColumn(family, c2, v2);
            put.addColumn(family, c3, v3);
            put.addColumn(family, c4, v4);
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
            TableName tn = TableName.valueOf("forecast");
            // 连接对象
            Connection connection = HbaseUtils.getCONNECTION();
            // 获取表对象
            Table results = connection.getTable(tn);
            // 定义Scan全表扫描对象
            Scan scan = new Scan();
            // 读取表中所有数据
            ResultScanner rows = results.getScanner(scan);
            // 打印表头
            System.out.println("气象站编号\t年份\t\t降雨量\t\t最高温度\t\t最低温度\t\t平均温度");
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

    public static void run() {
        try {
            //输入预测的日期
            Scanner scanner = new Scanner(System.in);
            System.out.print("请输入预测的日期(dd/MM)：");
            String date = scanner.nextLine();
            // 设置预测日期
            HbaseUtils.getCONF().set("date", date);
            // 定义输出表名
            String tableName = "forecast";
            // 创建输出表：存在，删除
            HbaseUtils.createTable(tableName, "info", true);
            // 构建Job对象
            Job job = Job.getInstance(HbaseUtils.getCONF(), "forecast");
            // 定义输入表名、列簇、列
            String weather = "weather";
            byte[] family = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("pecipitation");
            byte[] c2 = Bytes.toBytes("maxtemperature");
            byte[] c3 = Bytes.toBytes("mintemperature");
            byte[] c4 = Bytes.toBytes("avgtemperature");
            // 构建Scan对象
            Scan scan = new Scan();
            scan.addColumn(family, c1);
            scan.addColumn(family, c2);
            scan.addColumn(family, c3);
            scan.addColumn(family, c4);
            // 设置mapper
            TableMapReduceUtil.initTableMapperJob(weather, scan, Step7Mapper.class,
                    Text.class, Text.class, job);
            // 设置reducer
            TableMapReduceUtil.initTableReducerJob(tableName, Step7Reducer.class, job);
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
