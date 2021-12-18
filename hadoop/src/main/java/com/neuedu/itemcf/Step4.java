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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 第4步：2个矩阵相乘得到三维矩阵
 *
 * @author Alephant
 */
public class Step4 {
    public static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private String flag;// A同现矩阵 or B得分矩阵

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            if (flag.equals("cf3")) {// 同现矩阵
                //i100:i100    3
                String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];
                Text k = new Text(itemID1);// 个物品为key
                Text v = new Text("A:" + itemID2 + "," + num);
                context.write(k, v);
            } else if (flag.equals("cf2")) {//获取所有用户的喜欢矩阵
                //u2801    i1:1,i10:3
                String userID = tokens[0];// u13
                for (int i = 1; i < tokens.length; i++) {// i468:2,i446:3
                    String[] vector = tokens[i].split(":");
                    String itemID = vector[0];// 物品id i468
                    String pref = vector[1];// 喜爱分数 2
                    Text k = new Text(itemID); // 以物品为key
                    Text v = new Text("B:" + userID + "," + pref);
                    context.write(k, v);
                }
            }
        }
    }


    public static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> mapA = new HashMap<String, Integer>();
            Map<String, Integer> mapB = new HashMap<String, Integer>();
            for (Text line : values) {
                String val = line.toString();
                if (val.startsWith("A:")) {// 表示物品同现数字
                    final String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    try {
                        mapA.put(kv[0], Integer.parseInt(kv[1]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (val.startsWith("B:")) {// 评分
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    try {
                        mapB.put(kv[0], Integer.parseInt(kv[1]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();// itemID
                int num = mapA.get(mapk).intValue();
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();
                    int pref = mapB.get(mapkb).intValue();
                    result = num * pref;// 矩阵乘法相乘计算
                    Text k = new Text(mapkb);
                    Text v = new Text(mapk + "," + result);
                    context.write(k, v);
                }
            }
        }
    }


    public static void run(Map<String, String> paths) {
        try {
            // 定义输入目录
            Path inputPath1 = new Path(paths.get("step4_input1"));
            Path inputPath2 = new Path(paths.get("step4_input2"));
            // 定义输出目录
            Path outputPath = new Path(paths.get("step4_output"));
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("step4", Step4.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath1, inputPath2);
            // 设置mapper
            HadoopUtils.setMapper(job, Step4_Mapper.class, Text.class, Text.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, Step4_Reducer.class, Text.class, Text.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("第4步数完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
