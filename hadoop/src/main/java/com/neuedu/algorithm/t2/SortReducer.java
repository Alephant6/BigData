package com.neuedu.algorithm.t2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 算法——排序之升序：自定义Reducer
 * @author Alephant
 */
public class SortReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {
    private IntWritable id = new IntWritable(1);
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Reducer<IntWritable, NullWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        // 接受1： <32, null>
        // 接受2: <66,null>
        for (NullWritable v : values) {
            context.write(id, key);
            id = new IntWritable(id.get() + 1);
        }

    }
}
