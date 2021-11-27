package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class MapFileByWrite {
    public static void main(String[] args) {
        String[] data = {"One, two, buckle my shoe",
                "Three, four, shut the door", "Five, six, pick up sticks",
                "Seven, eight, lay them straight", "Nine, ten, a big fat hen"};
        // 1-构建配置对象
        Configuration conf = new Configuration();
        // 2-配置hadoop集群属性
        conf.set("fs.defaultFS","hdfs://master:9000");
        try{
            // 3-获取HDFS对象
            FileSystem fs = FileSystem.get(conf);
            // 定义目录
            Path dir = new Path("/map");
            // MapFile写入
            MapFile.Writer writer = new MapFile.Writer(conf, dir,
                    MapFile.Writer.keyClass(IntWritable.class),
                    MapFile.Writer.valueClass(Text.class), MapFile.Writer.compression(SequenceFile.CompressionType.NONE));

            for (int i = 0; i < data.length; i++) {
                writer.append(new IntWritable(i),new Text(data[i]));
            }

            IOUtils.closeStream(writer);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
