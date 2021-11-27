package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 逐字节查看文件内容：不推荐，效率太低
 * @author zhang
 */
public class Demo2 {
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
            // 循环读取：逐字节
            int c = reader.read();
            while (c != -1) {
                // 将字节转换成字符输出
                System.out.print((char) c);
                // 继续读取下一个字节内容
                c = reader.read();
            }
            // 关闭文件
            reader.close();
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
