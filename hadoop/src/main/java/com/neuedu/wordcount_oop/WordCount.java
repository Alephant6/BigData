package com.neuedu.wordcount_oop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义实体类WordCount: 多属性，不是一个单词
 *
 * @author Alephant
 */

public class WordCount implements WritableComparable<WordCount> {
    /**
     * word 属性
     */
    private String word;
    /**
     * 次数属性
     */
    private int count;

    /**
     * 无参构造器
     */
    public WordCount() {
    }

    /**
     * 带参构造器
     *
     * @param word 单词
     * @param count 次数
     */
    public WordCount(String word, int count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString(){
        // 重写toString，输出时会自动调用
        return this.word + "\t" + this.count;
    }
    @Override
    public int compareTo(WordCount o) {
        // 实现对象的比较，默认排序
        if (o == null){
            return 1;
        }
        return this.word.compareTo(o.word);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 实现hadoop序列化
        out.writeUTF(this.word);
        out.writeInt(this.count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 实现hadoop反序列
        this.word = in.readUTF();
        this.count = in.readInt();
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }



}
