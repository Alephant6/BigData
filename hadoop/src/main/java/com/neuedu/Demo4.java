package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * 缓冲读取器查看文件内容：专用于文本文件
 * @author zhang
 */
public class Demo4 {
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
            // 构建缓冲读取器对象
            BufferedReader br = new BufferedReader(new InputStreamReader(reader));
            // 循环读取每一行文本
            String line = br.readLine();
            while (null != line) {
                // 输出读取的一行文本
                System.out.println(line);
                // 继续读取
                line = br.readLine();
            }
            // 关闭读取器
            br.close();
            // 关闭文件
            reader.close();
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}

