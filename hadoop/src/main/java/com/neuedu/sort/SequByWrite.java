package com.neuedu.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SequByWrite {
    public static void main(String[] args) {
        // 1-构建配置对象
        Configuration conf = new Configuration();
        // 2-配置hadoop集群属性
        conf.set("fs.defaultFS","hdfs://master:9000");
        try{

        }catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
