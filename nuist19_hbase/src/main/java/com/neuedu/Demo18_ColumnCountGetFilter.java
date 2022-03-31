package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo18_ColumnCountGetFilter {
    public static void main(String[] args) {
        // 创建Configuration对象
        Configuration conf = HBaseConfiguration.create();
        // 设置集群属性：若提供hbase-site.xml时，自动读取配置信息
        // conf.set("hbase.zookeeper.property.clientPort","2181");
        // conf.set("hbase.zookeeper.quorum","master,slave1,slave2");
        try {
            // 创建Connection对象
            Connection conn = ConnectionFactory.createConnection(conf);
            // 定义表名
            String tableName = "employee";
            TableName tn = TableName.valueOf(tableName);
            // 获取表对象
            Table table = conn.getTable(tn);
            // 定义Scan全表扫描对象
            Scan scan = new Scan();

            // 定义过滤器对象
            // 仅返回符合条件的行键，和N列
            Filter filter = new ColumnCountGetFilter(2);


            // 设置过滤器
            scan.setFilter(filter);
            // 读取表中所有数据
            ResultScanner rows = table.getScanner(scan);
            // 打印表头
            System.out.println("学号\t\t年龄\t\t姓名\t\t性别");
            // 遍历所有行
            for (Result r : rows) {
                // 读取行键
                String id = Bytes.toString(r.getRow());
                System.out.print(id);
                // 遍历所有单元格：列簇、列、值
                for (Cell cell : r.rawCells()) {
                    // 读取行键
                    //String id = Bytes.toString(CellUtil.cloneRow(cell));
                    // 读取列族
                    //String f = Bytes.toString(CellUtil.cloneFamily(cell));
                    //System.out.print("\t列簇：" + f);
                    // 读取列名
                    String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                    //System.out.print("\t列名：" + c);
                    // 读取单元格的值：根据单元格的存储类型，进行特定的转换
                    switch (c) {
                        case "name":
                        case "sex":
                            String v1 = Bytes.toString(CellUtil.cloneValue(cell));
                            System.out.print("\t\t" + v1);
                            break;
                        case "age":
                            int v2 = Bytes.toInt(CellUtil.cloneValue(cell));
                            System.out.print("\t" + v2);
                            break;
                    }
                }
                System.out.println();
            }
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("失败~~~");
        }
    }
}
