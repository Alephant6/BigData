package com.neuedu.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 批量加载运行类
 *
 * @author alephant
 */
public class BulkLoadRun {
    public static void main(String[] args) {
        try {
            // 创建Configuration对象
            Configuration conf = HBaseConfiguration.create();
            // 设置集群属性：若提供hbase-site.xml时，自动读取配置信息
            // conf.set("hbase.zookeeper.property.clientPort","2181");
            // conf.set("hbase.zookeeper.quorum","master,slave1,slave2");
            // 定义输入路径
            String input = "hdfs://master:9000/emp/emp.txt";
            // 定义输出路径
            String output = "hdfs://master:9000/emp_output";
            // 定义表名
            String tableName = "emp";
            // 判断输出路径是否存在，存在则删除之
            // 特别提醒，不能使用hdfs编程代码，因为无效，但是代码无异常
            FileSystem hdfs = FileSystem.get(URI.create(output), conf);
            if (hdfs.exists(new Path(output))) {
                hdfs.delete(new Path(output), true);
            }
            hdfs.close();
            // 生成HFile,过程如下：
            // 构建Job
            Job job = Job.getInstance(conf, "bulkload");
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            // 设置mapper
            job.setMapperClass(BulkLoadMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            // 设置 reducer：可以省略
            // job.setReducerClass(PutSortReducer.class);
            // 设置输出：将/emp/emp.txt转换为HFile文件，并写入/emp_output
            job.setOutputFormatClass(HFileOutputFormat2.class);
            FileOutputFormat.setOutputPath(job, new Path(output));
            // 构建Connection对象
            Connection connection = ConnectionFactory.createConnection(conf);
            TableName tn = TableName.valueOf(tableName);
            Table table = connection.getTable(tn);
            // 获取表emp区域位置信息：表建立之初，都有一个region
            RegionLocator regionLocator = connection.getRegionLocator(tn);
            // 设置HFile
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("HFile生成成功~~~");
            }
            // 将HFile文件拷贝至hbase上，过程如下:
            ConnectionFactory.createConnection(conf);
            // 获取Admin
            Admin admin = connection.getAdmin();
            // loader = new LoadIncrementalHFiles(conf);
            //loader.doBulkLoad(new Path(output+"/info"), admin, table, regionLocator);
            BulkLoadHFilesTool tools = new BulkLoadHFilesTool(conf);
            tools.doBulkLoad(new Path(output+"/info"), admin, table, regionLocator);
            System.out.println("HFile拷贝成功~~~");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
