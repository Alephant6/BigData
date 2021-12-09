package com.neuedu.wordcount_order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义WordCountPartitioner分区：将map的结果发送给指定的reduce
 *
 * @author Alephant
 */
public class WordCountPartitioner extends Partitioner<WordCount, NullWritable> {
    @Override
    public int getPartition(WordCount text, NullWritable intWritable, int numPartitions) {
        //算法不能太复杂
        int result = Math.abs(text.toString().hashCode()) % numPartitions;
        return result;
    }
}
