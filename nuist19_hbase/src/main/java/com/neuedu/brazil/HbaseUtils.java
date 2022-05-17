package com.neuedu.brazil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Alephant
 */
public class HbaseUtils {
    private static Configuration CONF;
    private static Connection CONNECTION;

    static {
        try {
            // 创建Configuration对象
            CONF = HBaseConfiguration.create();
            // 设置集群属性：若提供hbase-site.xml时，自动读取配置信息
            CONF.set("hbase.zookeeper.property.clientPort", "2181");
            CONF.set("hbase.zookeeper.quorum", "master,slave1,slave2");
            // 创建Connection对象
            CONNECTION = ConnectionFactory.createConnection(CONF);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param familyName 列簇名称
     * @param isDelete 表存在，是否删除
     * @throws IOException
     */
    public static void createTable(String tableName, String familyName, boolean isDelete) throws IOException {
        try {
            Admin admin = CONNECTION.getAdmin();
            TableName tn = TableName.valueOf(tableName);
            // 判断表是否存在，存在则删除之
            if (admin.tableExists(tn) && isDelete) {
                // 先禁用表
                admin.disableTable(tn);
                // 后删除表
                admin.deleteTable(tn);
                System.out.println(tableName + " 表已存在，删除~~~");
            } else if (admin.tableExists(tn) && !isDelete) {
                // 表存在，不删除，后续无需创建表，直接返回即可
                System.out.println(tableName + " 表已存在，无需重新创建~~~");
                return;
            }
            // 创建表
            // 创建表结构对象的构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            // 创建列簇结构对象
            byte[] fn = Bytes.toBytes(familyName);
            // 创建列簇结构对象构造器
            ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(fn);
            // 生成列簇结构对象
            ColumnFamilyDescriptor cfd = cfdb.build();
            // 将列簇结构对象添加至表结构描述对象中
            tdb.setColumnFamily(cfd);
            // 生成表结构对象
            TableDescriptor td = tdb.build();
            // 创建表
            admin.createTable(td);
            System.out.println(tableName + " 表创建成功~~~");
        } catch (Exception ex) {
            throw ex;
        }
    }


    public static Configuration getCONF() {
        return CONF;
    }

    public static Connection getCONNECTION() {
        return CONNECTION;
    }


}
