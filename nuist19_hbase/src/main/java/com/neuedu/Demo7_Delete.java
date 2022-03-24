package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo7_Delete {
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
//             //操作1：删除某一行指定列簇中指定列
//            byte[] family = Bytes.toBytes("info");
//            byte[] c3 = Bytes.toBytes("age");
//            byte[] rk = Bytes.toBytes("1001");
//            // 实例化Delete对象
//            Delete delete = new Delete(rk);
//            // 指定列族中的某个列
//            delete.addColumn(family, c3);
//            // 执行删除操作
//            table.delete(delete);
            // 操作2：删除某一行指定列簇
//            byte[] family = Bytes.toBytes("info");
//            byte[] rk = Bytes.toBytes("1001");
//            // 实例化Delete对象
//            Delete delete = new Delete(rk);
//            // 指定列族
//            delete.addFamily(family);
//            // 执行删除操作
//            table.delete(delete);
            // 操作3：删除某一行
            byte[] rk = Bytes.toBytes("1002");
            // 实例化Delete对象
            Delete delete = new Delete(rk);
            // 执行删除操作
            table.delete(delete);

            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("失败~~~");
        }
    }
}
