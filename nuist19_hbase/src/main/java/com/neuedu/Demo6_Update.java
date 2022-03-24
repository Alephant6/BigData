package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Demo6_Update {
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
            // 构建Put对象:代表是某一行的数据
            // 定义列簇、列、单元格的值：全部转换为字节数组
            byte[] rk = Bytes.toBytes("1002");//行键
            byte[] family = Bytes.toBytes("info");//列族
//            byte[] c1 = Bytes.toBytes("name");//列
//            byte[] v1 = Bytes.toBytes("李四");//单元格的值
            byte[] c2 = Bytes.toBytes("sex");//列
            byte[] v2 = Bytes.toBytes("女");//单元格的值
//            byte[] c3 = Bytes.toBytes("age");//列
//            byte[] v3 = Bytes.toBytes(33);//单元格的值
            // 实例化Put对象
            Put put = new Put(rk);
            // 添加列值
//            put.addColumn(family, c1, v1);
            put.addColumn(family, c2, v2);
//            put.addColumn(family, c3, v3);
            // 向表中添加一行数据
            table.put(put);
            // 表关闭
            table.close();
            // 连接对象关闭
            conn.close();
            // 提示
            System.out.println("数据插入成功~~~");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("表创建失败~~~");
        }
    }
}
