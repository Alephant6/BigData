package com.neuedu.algorithm.t4;

import com.neuedu.algorithm.t3.SortedByDESC;
import com.neuedu.utils.HadoopUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

/**
 * 算法--TOPK
 * @author Alephant
 */
public class TOPK {
    public static class MyTopKMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {
        public int K = 3;
        private TreeMap<Long, Long> numbers;

        @Override
        protected void setup(Mapper<LongWritable, Text, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            // 初始化事件：mapper自动时自动执行
            numbers = new TreeMap<Long, Long>();
            String v = context.getConfiguration().get("topk");
            if (!StringUtils.isBlank(v)) {
                K = Integer.parseInt(v);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            // 源数据格式：1688
            // 为空处理
            String line = value.toString();
            if (StringUtils.isEmpty(line)) {
                return;
            }
            // 转换并添加至集合
            Long v =  Long.parseLong((line));
            numbers.put(v, v);
            // 控制数量并保证顺序：从大到小，所以移除第一个元素
            if (numbers.size() > K) {
                // 通过键移除元素：传递第一个元素的键
                numbers.remove(numbers.firstKey());
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            // 扫尾事件：map事件处理完成后，自动执行该事件
            // 输出集合中的所有元素
            for(Long v: numbers.values()) {
                context.write(NullWritable.get(), new LongWritable(v));
            }
        }
    }

    public static class MyTopKReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
        public int K = 3;
        private TreeMap<Long, Long> numbers;


        @Override
        protected void setup(Reducer<NullWritable, LongWritable, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            // 初始化事件：reducer自动时自动执行
            numbers = new TreeMap<Long, Long>();
            String v = context.getConfiguration().get("topk");
            if (!StringUtils.isBlank(v)) {
                K = Integer.parseInt(v);
            }
        }

        @Override
        protected void reduce(NullWritable key, Iterable<LongWritable> values, Reducer<NullWritable, LongWritable, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            for (LongWritable v : values) {
                // 添加至集合
                numbers.put(v.get(), v.get());
                // 控制数量并保证顺序：从大到小，所以移除第一个元素
                if (numbers.size() > K) {
                    // 通过键移除元素：传递第一个元素的键
                    numbers.remove(numbers.firstKey());
                }
            }
        }

        @Override
        protected void cleanup(Reducer<NullWritable, LongWritable, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            // 扫尾事件：reduce处理完成后，自动执行该事件
            // 输出集合中的所有集合：从大到小输出，集合中式默认升序
            for (Long v : numbers.descendingKeySet()) {
                context.write(NullWritable.get(), new LongWritable(v));
            }
        }
    }

    public static void main(String[] args) {
        try {
            // 定义TOPK中的K
            HadoopUtils.getConf().set("topk", "5");
            // 定义输入目录
            Path inputPath = new Path("/data2");
            // 定义输出目录
            Path outputPath = new Path("/data3_result");
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("TOPK", TOPK.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job, MyTopKMapper.class, NullWritable.class, LongWritable.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, MyTopKReducer.class, NullWritable.class, LongWritable.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("数据降序排序完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
