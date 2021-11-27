package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequByRead {
    public static void main(String[] args) {
        // 1-构建配置对象
        Configuration conf = new Configuration();
        // 2-配置hadoop集群属性
        conf.set("fs.defaultFS","hdfs://master:9000");
        try{
            // 3-获取HDFS对象
            FileSystem fs = FileSystem.get(conf);
            // SequenceFile写入
            Path dst = new Path("/seq.txt");
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(dst));
            //long position = reader.getPosition();
            IntWritable k = new IntWritable();
            Text v = new Text();
            while (reader.next(k,v)){
                System.out.println("key=" + k + ",value=" + v);
                //position = reader.getPosition();
            }
            IOUtils.closeStream(reader);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}

