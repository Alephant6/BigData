package com.neuedu.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 第2步：获取所有用户的喜欢(click、collect、cart、pay)矩阵
 *
 * @author Alephant
 */
public class Step2 {
    public static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //数据格式：i1,u2735,click,2014-9-3 16:23
            String[] tokens = value.toString().split(",");
            String item = tokens[0];//物品id
            String user = tokens[1];//用户id
            String action = tokens[2];//操作 click ,collect cart alipay
            Text k = new Text(user);//key: 用户id
            Integer rv = Starter.R.get(action);
            Text v = new Text(item + ":" + rv.intValue());//value:i1:1
            context.write(k, v);
        }
    }

    public static class Step2_Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> i, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //接收的数据：<"u2735",list<"i1:1","i2:3","i3:2","i1:2">>
            Map<String, Integer> r = new HashMap<String, Integer>();
            for (Text values : i) {
                String[] vs = values.toString().split(":");//value: i1:2
                String item = vs[0];//i1
                Integer action = Integer.parseInt(vs[1]);
                //判断是否存在重复物品：该物品被click、collect、cart、pay
                //存在评分累加
                action = ((Integer) (r.get(item) == null ? 0 : r.get(item))).intValue() + action;
                r.put(item, action);
            }
            //将所有喜欢的物品集合，拼接成一个字符串
            StringBuffer sb = new StringBuffer();
            for (Entry<String, Integer> entry : r.entrySet()) {
                sb.append(entry.getKey() + ":" + entry.getValue().intValue() + ",");
            }
            //去掉最后一个逗号
            context.write(key, new Text(sb.toString().substring(0, sb.toString().length() - 1)));
        }
    }


    public static void run(Map<String, String> paths) {
        try {
            // 定义输入目录
            Path inputPath = new Path(paths.get("step2_input"));
            // 定义输出目录
            Path outputPath = new Path(paths.get("step2_output"));
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("step2", Step2.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job, Step2_Mapper.class, Text.class, Text.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, Step2_Reducer.class, Text.class, Text.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("第2步数完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
