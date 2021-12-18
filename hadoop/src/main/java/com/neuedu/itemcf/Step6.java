package com.neuedu.itemcf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 第6步：按推荐值降序排序
 *
 * @author Alephant
 */
public class Step6 {
    private final static Text K = new Text();
    private final static Text V = new Text();

    public static class PairWritable implements WritableComparable<PairWritable> {
        private String uid;
        private double num;

        public PairWritable() {
        }

        public PairWritable(String uid, double num) {
            super();
            this.uid = uid;
            this.num = num;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.uid);
            out.writeDouble(this.num);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.uid = in.readUTF();
            this.num = in.readDouble();
        }

        @Override
        public int compareTo(PairWritable o) {
            int r = this.uid.compareTo(o.uid);
            if (r == 0) {
                return Double.compare(this.num, o.num);
            }
            return r;
        }

        public void setUid(String uid) {
            this.uid = uid;

        }

        public void setNum(double num) {
            this.num = num;

        }

        public String getUid() {

            return uid;
        }

        public double getNum() {
            return num;
        }
    }

    public static class Step6_Mapper extends Mapper<LongWritable, Text, PairWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // u2801 i1,5.0
            // u2801 i10,10.0
            // u3010 i1,2.0
            final String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            String u = tokens[0];
            String item = tokens[1];
            String num = tokens[2];
            PairWritable k = new PairWritable();
            k.setUid(u);
            k.setNum(Double.parseDouble(num));
            V.set(item + ":" + num);
            context.write(k, V);
        }
    }


    public static class NumSort extends WritableComparator {
        public NumSort() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable p1 = (PairWritable) a;
            PairWritable p2 = (PairWritable) b;
            int r = p1.getUid().compareTo(p2.getUid());
            if (r == 0) {
                return -Double.compare(p1.getNum(), p2.getNum());
            }
            return r;
        }
    }

    public static class UserGroup extends WritableComparator {
        public UserGroup() {
            super(PairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairWritable p1 = (PairWritable) a;
            PairWritable p2 = (PairWritable) b;
            return p1.getUid().compareTo(p2.getUid());
        }
    }

    public static class Step6_Reducer extends Reducer<PairWritable, Text, Text, Text> {
        @Override
        protected void reduce(PairWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int i = 0;
            StringBuffer sb = new StringBuffer();
            for (Text v : values) {
                if (i == 10) {
                    break;
                }
                sb.append(v.toString() + ",");
                i++;
            }
            K.set(key.getUid());
            V.set(sb.toString());
            context.write(K, V);
        }
    }

    public static void run(Map<String, String> paths) {
        try {
            // 定义输入目录
            Path inputPath = new Path(paths.get("step6_input"));
            // 定义输出目录
            Path outputPath = new Path(paths.get("step6_output"));
            // 输出目录存在之则删除
            HadoopUtils.deletePath(outputPath);
            // 创建任务
            Job job = HadoopUtils.getJob("step6", Step6.class);
            // 设置输入
            HadoopUtils.setInput(job,TextInputFormat.class,inputPath);
            // 设置mapper
            job.setMapperClass(Step6_Mapper.class);
            job.setMapOutputKeyClass(PairWritable.class);
            job.setMapOutputValueClass(Text.class);
            //设置排序
            job.setSortComparatorClass(NumSort.class);
            //设置分组
            job.setGroupingComparatorClass(UserGroup.class);

            // 设置Reducer
            HadoopUtils.setReducer(job, Step6_Reducer.class, Text.class, Text.class);
            // 设置输出
            HadoopUtils.setOutput(job, TextOutputFormat.class, outputPath);

            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("第6步数完成，数据显示如下：");
                HadoopUtils.showContentOfPath(outputPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
