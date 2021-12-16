package com.neuedu.algorithm.t5;

import com.neuedu.utils.HadoopUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 算法--倒排索引
 *
 * @author Alephant
 */
public class InvertIndex {
    public static class MyInvertIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text one = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 将Text转换为String
            String line = value.toString();
            // 数据清洗：不能为空
            if (StringUtils.isBlank(line)) {
                return;
            }
            // 获取文件名：也可以理解为图书URI
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            // 分
            // 简单的单词拆分工具类
            StringTokenizer st = new StringTokenizer(line);
            while (st.hasMoreTokens()) {
                // 获取单词
                String word = st.nextToken();
                // 输出结果：<"china@file1.txt","1">
                context.write(new Text(word + "@" + fileName), one);
            }
        }
    }

    public static class MyInvertIndexCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //接收本机map输出的数据：做一个本机的小计汇总
            // <"china@file1.txt",list<"1">>
            // <"me@file1.txt",list<"1","1","1">>
            int count = 0;
            for (Text v : values) {
                count++;
            }
            //拆分key为单词和文件名："china@file1.txt"，"me@file1.txt"
            //0是单词，1是文件名
            String[] items = key.toString().split("@");
            //输出：<单词,文件名:次数>
            context.write(new Text(items[0]), new Text(items[1] + ":" + count));
        }
    }

    public static class MyInvertIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //接收所有服务器归并输出的结果
            // <"china",list<"file1.txt:1">>
            // <"me",list<"file1.txt:1","file2.txt:1","file3.txt:1">>
            //输出：<单词,values拼接>
            // "file1.txt:1;file2.txt:1;file3.txt:1"
            StringBuffer sb = new StringBuffer();
            for (Text v : values) {
                sb.append(v.toString() + ";");
            }
            //输出：<"me","file1.txt:1;file2.txt:1;file3.txt:1">
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {
        try {
            //定义TOPK中的K
            HadoopUtils.getConf().set("topk", "5");
            // 定义输入目录
            Path inputPath = new Path("/mybooks");
            // 定义输出目录
            Path outputPath = new Path("/mybooks_result");
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("InvertIndex", InvertIndex.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job, MyInvertIndexMapper.class, Text.class, Text.class);
            // 设置归并
            job.setCombinerClass(MyInvertIndexCombiner.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, MyInvertIndexReducer.class, Text.class, Text.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("倒排索引完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
