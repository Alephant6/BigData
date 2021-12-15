package com.neuedu.algorithm.t3;

import com.neuedu.utils.HadoopUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 算法--排序之降序
 *
 * @author alephant
 */
public class SortedByDESC {

    /**
     * 自定义Mpper
     */
    public static class SortedMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            // 源数据格式：1688
            // 为空处理
            String line = value.toString();
            if (StringUtils.isEmpty(line)) {
                return;
            }
            //输出
            context.write(new IntWritable(Integer.valueOf(line)), NullWritable.get());
        }
    }

    /**
     * 自定义Reducer
     */
    public static class SortedReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
        private IntWritable id = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Reducer<IntWritable, NullWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            // 接收1：<32,<null>>
            // 接收2：<66,<null,null,null>>
            for (NullWritable v : values) {
                // 输出:<id,num>
                context.write(id, key);
                id = new IntWritable(id.get() + 1);
            }
        }
    }

    /**
     * 自定义排序类
     */
    public static class SortedComparator extends IntWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) {
        try {
            // 定义输入目录
            Path inputPath = new Path("/data2");
            // 定义输出目录
            Path outputPath = new Path("/data2_2_result");
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("sort-desc", SortedByDESC.class);
            // 设置输入
            HadoopUtils.setInput(job, TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job, SortedMapper.class, IntWritable.class, NullWritable.class);
            // 设置排序
            HadoopUtils.setSortComparator(job,SortedComparator.class);
            // 设置Reducer
            HadoopUtils.setReducer(job, SortedReducer.class, IntWritable.class, IntWritable.class);
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
