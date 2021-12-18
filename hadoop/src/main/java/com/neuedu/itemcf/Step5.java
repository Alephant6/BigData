package com.neuedu.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 第5步：三维矩阵的数据相加，获得所有用户对所有物品的总推荐值
 *
 * @author Alephant
 */
public class Step5 {
    public static class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            Text k = new Text(tokens[0]);// 用户为key
            Text v = new Text(tokens[1] + "," + tokens[2]);
            context.write(k, v);
        }
    }

    public static class Step5_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Double> map = new HashMap<String, Double>();// 结果
            for (Text line : values) {
                String[] tokens = line.toString().split(",");
                String itemID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                if (map.containsKey(itemID)) {
                    map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
                } else {
                    map.put(itemID, score);
                }
            }
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter.next();
                double score = map.get(itemID);
                Text v = new Text(itemID + "," + score);
                context.write(key, v);
            }
        }
    }

    public static void run(Map<String, String> paths) {
        try {
            // 定义输入目录
            Path inputPath = new Path(paths.get("step5_input"));
            // 定义输出目录
            Path outputPath = new Path(paths.get("step5_output"));
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("step5", Step5.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job, Step5_Mapper.class, Text.class, Text.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, Step5_Reducer.class, Text.class, Text.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("第5步数完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
