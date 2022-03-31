package com.neuedu.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


/**
 * 运行类
 *
 * @author Alephant
 */
public class WordCount2Run {
    public static void main(String[] args) {
        try {
            // 创建Configuration对象
            Configuration conf = HBaseConfiguration.create();
            // 设置集群属性：若提供hbase-site.xml时，自动读取配置信息
            // conf.set("hbase.zookeeper.property.clientPort","2181");
            // conf.set("hbase.zookeeper.quorum","master,slave1,slave2");
            // 定义源表名
            String sourceTable = "books";
            // 定义目标表名
            String targetTable = "wordcount";
            // 定义输出目标表
            conf.set(TableOutputFormat.OUTPUT_TABLE, targetTable);
            // 定义读取源表中的哪些列簇、列
            byte[] f = Bytes.toBytes("info");
            byte[] c = Bytes.toBytes("line");
            // 定义Scan对象，并设置读取源表中的哪些列簇、列
            Scan scan = new Scan();
            scan.addColumn(f, c);
            // 构建Job对象
            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCount2Run.class);
            // 设置mappper
            TableMapReduceUtil.initTableMapperJob(sourceTable, scan, WordCountTableMapper.class, Text.class, IntWritable.class, job);
            // 设置reducer
            TableMapReduceUtil.initTableReducerJob(targetTable, WordCountTableReducer.class, job);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("hbase版本词频统计结束~~~");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

