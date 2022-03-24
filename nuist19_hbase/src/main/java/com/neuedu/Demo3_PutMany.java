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
import java.util.ArrayList;
import java.util.List;

public class Demo3_PutMany {
    public static void main(String[] args) {
        // 创建Configuration对象
        Configuration conf = HBaseConfiguration.create();
        // 设置集群属性：若提供hbase-site.xml时，自动读取配置信息
        // conf.set("hbase.zookeeper.property.clientPort","2181");
        // conf.set("hbase.zookeeper.quorum","master,slave1,slave2");
        // 开始时间
        long begin = System.currentTimeMillis();
        try {
            // 创建Connection对象
            Connection conn = ConnectionFactory.createConnection(conf);
            // 定义表名
            String tableName = "test";
            TableName tn = TableName.valueOf(tableName);
            // 获取表对象
            Table table = conn.getTable(tn);
            // 构建Put对象:代表是某一行的数据
            List<Put> putList = new ArrayList<>();
            for (int i = 1; i <= 1000000; i++) {
                // 定义列簇、列、单元格的值：全部转换为字节数组
                byte[] rk = Bytes.toBytes("" + i);//行键
                byte[] family = Bytes.toBytes("info");//列族
                byte[] c1 = Bytes.toBytes("content");//列
                byte[] v1 = Bytes.toBytes("value:" + i);//单元格的值
                // 实例化Put对象
                Put put = new Put(rk);
                // 添加列值
                put.addColumn(family, c1, v1);
                // 添加至集合
                putList.add(put);
                // 数据量达到1万时，新增
                if (i % 10000 == 0) {
                    table.put(putList);
                    // 清空集合
                    putList.clear();
                }
            }
            // 向表中添加N行数据
            table.put(putList);
            // 结束时间
            long end = System.currentTimeMillis();
            // 表关闭
            table.close();
            // 连接对象关闭
            conn.close();
            // 提示
            System.out.println("百万数据插入成功~~~所需时间："+(end-begin)/1000);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("失败~~~");
        }
    }
}

