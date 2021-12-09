package com.neuedu.wordcount_oop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;


public class Run {
    public static void main(String[] args) {
        // 构建配置对象
        Configuration conf = new Configuration();
        // 配置hadoop集群属性
        conf.set("fs.defaultFS", "hdfs://master:9000");
        try {
            // 获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            // 定义输入目录
            Path inputPath = new Path("/books");
            // 定义输出目录
            Path outputPath = new Path("/wordcount_result");
            // 输出目录存在之则删除
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            // 创建任务
            Job job = Job.getInstance(conf, "wordcount");
            // 设置运行类
            job.setJarByClass(com.neuedu.wordcount_oop.Run.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,inputPath);
            // 设置Mapper
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(WordCount.class);
            job.setMapOutputValueClass(NullWritable.class);
            // 设置Reducer
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(WordCount.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            // 查看结果
            System.out.println("词频统计结束\n单词\t次数");
            for (FileStatus s : hdfs.listStatus(outputPath)){
                // 目录包含子目录、文件
                // FileStatus方法getXXX获取指定信息
                // FileStatus方法isXXX判断
                if (s.isFile()){
                    // 打开文件
                    FSDataInputStream reader = hdfs.open(s.getPath());
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
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
