package com.neuedu.wordcount_order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义排序类GroupByWord：与实现自定义排序的过程是一样的，只不过，分组只关注相等情况，也就是0
 *
 * @author Alephant
 */

public class GroupByWord extends WritableComparator {
    public GroupByWord() {super(WordCount.class, true);}

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WordCount w1 = (WordCount) a;
        WordCount w2 = (WordCount) b;
        // 按字母升序分组
        return  w1.getWord().compareTo(w2.getWord());
    }
}
