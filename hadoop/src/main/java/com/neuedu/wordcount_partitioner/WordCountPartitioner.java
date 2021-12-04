package com.neuedu.wordcount_partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义WordCountPartitioner分区：将map的结果发送给指定的reduce
 *
 * @author Alephant
 */
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        //算法不能太复杂
        int result = Math.abs(text.toString().hashCode()) % numPartitions;
        return result;
    }
}
