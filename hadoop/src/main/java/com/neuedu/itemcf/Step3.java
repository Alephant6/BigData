package com.neuedu.itemcf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 第3步：获取所有物品之间的同现矩阵
 *
 * @author Alephant
 */
public class Step3 {
    private final static Text K = new Text();
    private final static IntWritable V = new IntWritable(1);

    public static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            String[] tokens = value.toString().split("\t");
            String[] items = tokens[1].split(",");//i468:2 i446:3
            for (int i = 0; i < items.length; i++) {
                String itemA = items[i].split(":")[0];//i468:2
                for (int j = 0; j < items.length; j++) {
                    String itemB = items[j].split(":")[0];//i468
                    K.set(itemA + ":" + itemB);//i468:i446 //i446:i468
                    context.write(K, V);
                }
            }
        }
    }

    public static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum = sum + v.get();
            }
            V.set(sum);
            context.write(key, V);
        }
    }


    public static void run(Map<String, String> paths) {
        try {
            // 定义输入目录
            Path inputPath = new Path(paths.get("step3_input"));
            // 定义输出目录
            Path outputPath = new Path(paths.get("step3_output"));
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("step3", Step3.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job, Step3_Mapper.class, Text.class, IntWritable.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, Step3_Reducer.class, Text.class, IntWritable.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("第3步数完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
