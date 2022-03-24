package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Alephant
 */
public class Demo1_CreateTable {
    public static void main(String[] args) {
        // 创建Configuration对象
        Configuration conf = HBaseConfiguration.create();
        // 设置集群属性：若提供hbase-site.xml时，自动读取配置信息
        // conf.set("hbase.zookeeper.property.clientPort","2181");
        // conf.set("hbase.zookeeper.quorum","master,slave1,slave2");
        try {
            // 创建Connection对象
            Connection conn = ConnectionFactory.createConnection(conf);
            // 获取Admin对象
            Admin admin = conn.getAdmin();
            // 定义表名
            String tableName = "employee";
            TableName tn = TableName.valueOf(tableName);
            // 判断表是否存在，存在则删除之
            if (admin.tableExists(tn)) {
                // 先禁用表
                admin.disableTable(tn);
                // 后删除表
                admin.deleteTable(tn);
            }
            // 创建表结构对象的构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            // 创建列簇结构对象
            String familyName = "info";
            byte[] fn = Bytes.toBytes(familyName);
            // 创建列簇结构对象构造器
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(fn);
            // 生成列簇结构对象
            ColumnFamilyDescriptor cfd = cfdb.build();
            // 将列簇结构对象添加至表结构描述对象中
            tdb.setColumnFamily(cfd);
            // 添加多个列簇结构对象
//            tdb.setColumnFamilies(集合)
            // 生成表结构对象
            TableDescriptor td = tdb.build();
            // 创建表
            admin.createTable(td);
            System.out.println("表创建成功~~~");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("表创建失败~~~");
        }

    }
}

