package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Alephant
 */
public class Demo1 {
    public static void main(String[] args) {
        // 1-构建配置对象
        Configuration conf = new Configuration();
        // 2-配置hadoop集群属性
        conf.set("fs.defaultFS","hdfs://master:9000");
        try{
            // 3-获取HDFS对象
            FileSystem fs = FileSystem.get(conf);
            // 4-定义源文件：本地文件路径
            Path src = new Path("file/f.txt");
            // 5-定义目标文件：HDFS中文件的路径,/是根目录
            Path dst = new Path("/f.txt");
            // 6-上传文件
            fs.copyFromLocalFile(src, dst);
            // 7-提示信息
            System.out.println("文件上传成功~~~");
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}

