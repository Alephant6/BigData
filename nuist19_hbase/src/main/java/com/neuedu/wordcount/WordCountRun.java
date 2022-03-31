package com.neuedu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 运行类
 * @author Alephant
 */
public class WordCountRun {
    public static void main(String[] args) {
        // 创建Configuration对象
        Configuration conf = HBaseConfiguration.create();
        // 设置集群属性：若提供hbase-site.xml时，自动读取配置信息
        // conf.set("hbase.zookeeper.property.clientPort","2181");
        // conf.set("hbase.zookeeper.quorum","master,slave1,slave2");
        try {
            // 提前创建词频信息写入的表wordcount
            conf.set(TableOutputFormat.OUTPUT_TABLE,"wordcount");
            // 创建job
            Job job = Job.getInstance(conf);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,new Path("hdfs://master:9000/books"));
            // 设置Mapper
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 设置Reducer
            job.setReducerClass(WordCountReducer.class);
            // 设置输出
            job.setOutputFormatClass(TableOutputFormat.class);
            // 执行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("hbase版本的词频统计结束~~~");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

