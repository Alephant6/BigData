package com.neuedu.weather;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 运行类
 * @author Alephant
 */
public class Run2 {
    public static void main(String[] args) {
        try{
            // 定义输入目录
            Path inputPath = new Path("/weather_data");
            // 定义输出目录
            Path outputPath = new Path("/weather_result");
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.createJob("weather", Run2.class);
            // 设置输入
            HadoopUtils.setIn(job, inputPath,TextInputFormat.class);
            // 设置Mapper
            HadoopUtils.setMapper(job, WeatherMapper.class, WeatherWritable.class, NullWritable.class);
            // 设置分区
            HadoopUtils.setPartitioner(job, 3, WeatherPartitioner.class);
            // 设置自定义排序
            HadoopUtils.setSort(job, SortedByHotDESC.class);
            // 设置自定义分组
            HadoopUtils.setGroup(job, GroupByYear.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, WeatherReducer.class, WeatherWritable.class, NullWritable.class);
            // 设置输出
            HadoopUtils.setOut(job, outputPath, TextOutputFormat.class);
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
