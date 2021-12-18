package com.neuedu.itemcf;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 第1个MR：数据清洗
 *
 * @author Alephant
 */
public class Step1 {

    public static class MyStep1Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            //判断为空:null、空字符串、N个空格组成的字符串
            if (StringUtils.isBlank(line)) {
                return;
            }
            //数据格式是否正确：i1,u2735,click,2014-9-3 16:23
            if (line.split(",").length != 4) {
                return;
            }
            //输出：整行作为键输出，自动去重
            context.write(value, NullWritable.get());
        }
    }

    public static class MyStep1Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
                              Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            //接收到的数据键，其实自动去重，键是唯一的，直接输出键
            //键就是整行，实现了行的去重功能
            context.write(key, NullWritable.get());
        }
    }

    public static void run(Map<String, String> paths) {
        try {
            // 定义输入目录
            Path inputPath = new Path(paths.get("step1_input"));
            // 定义输出目录
            Path outputPath = new Path(paths.get("step1_output"));
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("step1", Step1.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job, MyStep1Mapper.class, Text.class, NullWritable.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, MyStep1Reducer.class, Text.class, NullWritable.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("第1步数完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
