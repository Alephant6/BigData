package com.neuedu.weather;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 运行类
 * @author Alephant
 */
public class Run {
    public static void main(String[] args) {
        try{
            // 定义输入目录
            Path inputPath = new Path("/weather_data");
            // 定义输出目录
            Path outputPath = new Path("/weather_result");
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = Job.getInstance(HadoopUtils.getConf(), "weather");
            job.setJarByClass(Run.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, inputPath);
            // 设置Mapper
            job.setMapperClass(WeatherMapper.class);
            job.setMapOutputKeyClass(WeatherWritable.class);
            job.setMapOutputValueClass(NullWritable.class);
            // 设置分区
            job.setPartitionerClass(WeatherPartitioner.class);
            job.setNumReduceTasks(3);
            // 设置自定义排序
            job.setSortComparatorClass(SortedByHotDESC.class);
            // 设置自定义分组
            job.setGroupingComparatorClass(GroupByYear.class);
            // 设置Reducer
            job.setReducerClass(WeatherReducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("年份\t\t温度");
                HadoopUtils.showContentOfPath(outputPath);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
