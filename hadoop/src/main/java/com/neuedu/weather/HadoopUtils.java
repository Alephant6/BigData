package com.neuedu.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 封装工具类HadoopUtils
 * 功能：
 * 构建配置对象
 * 获取HDFS文件系统
 * 删除目录
 * 读取指定目录下所有文件内容
 * ……
 *
 * @author Alephant
 */
public class HadoopUtils {
    private static Configuration conf;
    private static FileSystem hdfs;

    /**
     * 静态语句块，类加载时执行：构建配置对象，获取HDFS文件系统
     */
    static {
        try {
            // 构建配置对象
            conf = new Configuration();
            // 配置hadoop集群属性
            conf.set("fs.defaultFS", "hdfs://master:9000");
            // 获取HDFS对象
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回HDFS文件系统
     * @return HDFS文件系统
     * @throws IOException
     */
    public static FileSystem getFileSystem() throws IOException {
        return hdfs;
    }

    /**
     * 删除指定路径
     * @param dst 路径
     * @throws IOException
     */
    public static void deletePath(Path dst) throws IOException {
        // 输出目录存在之则删除
        if (hdfs.exists(dst)) {
            hdfs.delete(dst, true);
        }
    }

    /**
     * 读取指定路径内的所有文件内容并输出至控制台
     * @param dst
     * @throws IOException
     */
    public static void showContentOfPath(Path dst) throws IOException {
        for (FileStatus s : hdfs.listStatus(dst)) {
            // 目录包含子目录、文件
            // FileStatus方法getXXX获取指定信息
            // FileStatus方法isXXX判断
            if (s.isFile()) {
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
    }

    /**
     * 返回配置对象，用于后期为集群添加参数
     * @return
     */
    public static Configuration getConf() {
        return conf;
    }

    /**
     * 创建任务
     * @param jobName 任务名
     * @param cls 运行类
     * @return
     * @throws IOException
     */
    public static Job createJob(String jobName, Class<?> cls) throws IOException {
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(cls);
        return job;
    }

    /**
     * 设置输入
     * @param job
     * @param inputPath
     * @param cls
     * @throws IOException
     */
    public static void setIn(Job job, Path inputPath, Class<? extends InputFormat> cls) throws IOException {
        job.setInputFormatClass(cls);
        FileInputFormat.setInputPaths(job, inputPath);
    }

    /**
     * 设置Mapper
     * @param job
     * @param cls1
     * @param cls2
     * @param cls3
     */
    public static void setMapper(Job job, Class<? extends Mapper> cls1, Class<?> cls2, Class<?> cls3){
        job.setMapperClass(cls1);
        job.setMapOutputKeyClass(cls2);
        job.setMapOutputValueClass(cls3);
    }

    /**
     * 设置分区
     * @param job
     * @param num
     * @param cls
     */
    public static void setPartitioner(Job job, int num, Class<? extends Partitioner> cls){
        job.setPartitionerClass(cls);
        job.setNumReduceTasks(num);
    }

    /**
     * 设置自定义排序
     * @param job
     * @param cls
     */
    public static void setSort(Job job, Class<? extends RawComparator> cls){
        // 设置自定义排序
        job.setSortComparatorClass(cls);
    }

    /**
     * 设置自定义分组
     * @param job
     * @param cls
     */
    public static void setGroup(Job job, Class<? extends RawComparator> cls){
        // 设置自定义分组
        job.setGroupingComparatorClass(cls);
    }

    public static void setReducer(Job job, Class<? extends Reducer> cls1, Class<?> cls2, Class<?> cls3){
        // 设置Reducer
        job.setReducerClass(cls1);
        job.setOutputKeyClass(cls2);
        job.setOutputValueClass(cls3);
    }

    public static void  setOut(Job job, Path outputPath, Class<? extends OutputFormat> cls)throws IOException {
        job.setOutputFormatClass(cls);
        FileOutputFormat.setOutputPath(job, outputPath);
    }


}
