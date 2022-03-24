package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo4_Get {
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
            // 定义列簇、列
            byte[] family = Bytes.toBytes("info");
            byte[] c1 = Bytes.toBytes("name");
            byte[] c2 = Bytes.toBytes("sex");
            byte[] c3 = Bytes.toBytes("age");
            // 定义读取的记录的行键
            byte[] rk = Bytes.toBytes("1001");
            // 构建Get对象
            Get get = new Get(rk);
            // 添加列族、列
            get.addFamily(family);
            get.addColumn(family, c1);
            get.addColumn(family, c2);
            get.addColumn(family, c3);
            // 读取数据,仅返回一行
            Result result = table.get(get);
            // 遍历一行中的所有列
            for (Cell cell : result.rawCells()) {
                // 读取行键
                String id = Bytes.toString(result.getRow());
                //String id = Bytes.toString(CellUtil.cloneRow(cell));
                System.out.print("行键："+id);
                // 读取列族
                String f = Bytes.toString(CellUtil.cloneFamily(cell));
                System.out.print("\t列簇："+f);
                // 读取列名
                String c = Bytes.toString(CellUtil.cloneQualifier(cell));
                System.out.print("\t列名："+c);
                // 读取单元格的值：根据单元格的存储类型，进行特定的转换
                switch (c) {
                    case "name":
                    case "sex":
                        String v1 = Bytes.toString(CellUtil.cloneValue(cell));
                        System.out.print("\t单元格的值："+v1);
                        break;
                    case "age":
                        int v2 = Bytes.toInt(CellUtil.cloneValue(cell));
                        System.out.print("\t单元格的值："+v2);
                        break;
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("失败~~~");
        }
    }
}

