package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 按字节块查看文件内容：任何文件类型均合适，特别擅长二进制
 * @author Alephant
 */
public class Demo3 {
    public static void main(String[] args) {
        // 1-构建配置对象
        Configuration conf = new Configuration();
        // 2-配置hadoop集群属性
        conf.set("fs.defaultFS","hdfs://master:9000");
        try{
            // 3-获取HDFS对象
            FileSystem fs = FileSystem.get(conf);
            // 定义目标文件：HDFS中文件的路径,/是根目录
            Path dst = new Path("/f.txt");
            // 打开文件
            FSDataInputStream reader = fs.open(dst);
            // 文件大小
            int length = reader.available();
            // 定义块
            byte[] block = new byte[1024];
            // 计算块的个数
            int max = length / 1024;
            // 计算最后一个块大小：前面是1024的倍数，最后一个块小于1024
            byte[] last_block = new byte[length % 1024];
            // 循环读取所有块
            for (int i = 0; i < max; i++) {
                // 读取块
                int flag = reader.read(block, 0, block.length);
                // 将字节数组转换为字符串输出
                System.out.print(new String(block));
            }
            // 读取最后一个块
            if (last_block.length > 0) {
                // 读取最后一块
                int flag = reader.read(last_block, 0, last_block.length);
                // 将字节数组转换为字符串输出
                System.out.print(new String(last_block));
            }
            // 关闭文件
            reader.close();
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}

