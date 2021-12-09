package com.neuedu.wordcount_order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义排序类SortedByWordDESC：实现按单词的字母降序排列
 *
 * @author Alephant
 */
public class SortedByWordDESC extends WritableComparator {
    public SortedByWordDESC() {
        super(WordCount.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WordCount w1 = (WordCount) a;
        WordCount w2 = (WordCount) b;
        // 按字母降序排列
        return -w1.getWord().compareTo(w2.getWord());
    }
}
