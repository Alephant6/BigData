package com.neuedu.brazil;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Scanner;

/**
 * 查询指定日期的天气数据
 *
 * @author Alephant
 */
public class Step2 {
    /**
     * 查询指定日期的天气数据
     *
     * @param tableName 表名
     * @param date      查询日期
     */
    public static void search(String tableName, String date) {
        try {
            // 构建TableName对象
            TableName tn = TableName.valueOf(tableName);
            // 连接对象
            Connection connection = HbaseUtils.getCONNECTION();
            // 获取表对象
            Table weather = connection.getTable(tn);
            // 定义Scan全表扫描对象
            Scan scan = new Scan();
            // 定义比较器
            ByteArrayComparable comparator = new SubstringComparator(date);
            // 定义过滤器对象
            Filter filter = new RowFilter(CompareOperator.EQUAL, comparator);
            // 设置过滤器
            scan.setFilter(filter);
            // 读取表中所有数据
            ResultScanner rows = weather.getScanner(scan);
            // 打印表头
            System.out.println("气象站编号\t\t日期\t\t降雨量\t\t最高温度\t\t最低温度\t\t平均温度");
            // 定义变量
            String code = "";
            Float pecipitation = 0.0F;
            Float maxtemperature = 0.0F;
            Float mintemperature = 0.0F;
            Float avgtemperature = 0.0F;
            // 遍历所有行
            for (Result r : rows) {
                // 读取行键
                String rowKey = Bytes.toString(r.getRow());
                code = rowKey.split("_")[0];
                // date = rowKey.split("_")[1];
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
                System.out.println(code + "\t\t" + date + "\t\t" + pecipitation + "\t\t" + maxtemperature +
                        "\t\t" + mintemperature + "\t\t" + avgtemperature);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // 定义表名
        String tableName = "weather";
        // 输入指定日期
        Scanner scanner = new Scanner(System.in);
        System.out.print("请输入查询的日期(DD/MM/YYYY)：");
        String date = scanner.nextLine();
        // 调用查询方法并输出结果
        search(tableName, date);
    }
}
