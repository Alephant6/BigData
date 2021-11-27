package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class MapFileByRead {
    public static void main(String[] args) {
        // 1-构建配置对象
        Configuration conf = new Configuration();
        // 2-配置hadoop集群属性
        conf.set("fs.defaultFS", "hdfs://master:9000");
        try {
            // 3-获取HDFS对象
            FileSystem fs = FileSystem.get(conf);
            // 定义目录
            Path dir = new Path("/map");
            // MapFile写入
            MapFile.Reader reader = new MapFile.Reader(dir, conf);
            IntWritable k = new IntWritable();
            Text v = new Text();
            while (reader.next(k, v)) {
                System.out.println("key=" + k + ",value=" + v);
            }
            IOUtils.closeStream(reader);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

