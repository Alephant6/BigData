package com.neuedu.algorithm.t1;

import com.neuedu.utils.HadoopUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 算法--数据去重：运行类
 * @author Alephant
 */
public class NotRepeatRun {
    public static void main(String[] args) {
        try {
            // 定义输入目录
            Path inputPath = new Path("/data1");
            // 定义输出目录
            Path outputPath = new Path("/data1_result");
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("notrepeat", NotRepeatRun.class);
            // 设置输入
            HadoopUtils.setInput(job,TextInputFormat.class, inputPath);
            // 设置mapper
            HadoopUtils.setMapper(job,NotRepeatMapper.class,Text.class,NullWritable.class);
            // 设置Reducer
            HadoopUtils.setReducer(job,NotRepeatReducer.class,Text.class,NullWritable.class);
            // 设置输出
            HadoopUtils.setOutput(job,TextOutputFormat.class,outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("数据去重完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
