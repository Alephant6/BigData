package com.neuedu.utils;

import com.neuedu.algorithm.t3.SortedByDESC;
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
     * 创建job任务
     * @param jobName 任务名称
     * @param cls jar运行类
     * @return job对象
     * @throws IOException
     */
    public static Job getJob(String jobName,Class<?> cls) throws IOException {
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(cls);
        return job;
    }

    /**
     * 设置输入
     * @param job job
     * @param cls 输入格式
     * @param dst 输入路径
     * @throws IOException
     */
    public static void setInput(Job job,Class<? extends InputFormat> cls,Path dst) throws IOException {
        job.setInputFormatClass(cls);
        FileInputFormat.setInputPaths(job, dst);
    }

    /**
     * 设置mapper及其输出
     * @param job job
     * @param mapperClass mapper
     * @param mapOutputKeyClass key类型
     * @param mapOutputValueClass value类型
     */
    public static void setMapper(Job job,Class<? extends Mapper> mapperClass,Class<?> mapOutputKeyClass,Class<?> mapOutputValueClass) {
        job.setMapperClass(mapperClass);
        job.setMapOutputValueClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
    }

    /**
     * 设置reducer及其输出
     * @param job job
     * @param reducerClass reducer
     * @param outputKeyClass key类型
     * @param outputValueClass value类型
     */
    public static void setReducer(Job job, Class<? extends Reducer> reducerClass, Class<?> outputKeyClass, Class<?> outputValueClass) {
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
    }

    /**
     * 设置输出
     * @param job job
     * @param cls 输出格式
     * @param dst 输出路径
     */
    public static void setOutput(Job job,Class<? extends OutputFormat> cls,Path dst){
        job.setOutputFormatClass(cls);
        FileOutputFormat.setOutputPath(job, dst);
    }

    public static void setSortComparator(Job job, Class<? extends RawComparator> cls) {
        job.setSortComparatorClass(cls);
    }

    /**
     * 返回配置对象，用于后期为集群添加参数
     * @return
     */
    public static Configuration getConf() {
        return conf;
    }

}
