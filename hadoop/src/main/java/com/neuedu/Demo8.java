package com.neuedu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.nio.charset.StandardCharsets;


public class Demo8 {
    public static void main(String arg[]){
        IntWritable v = new IntWritable(6);
        System.out.println(v);
        int i = v.get();
        System.out.println(i);
        // Text与JAVA中String相似，均使用Unic（utf-8, utf-16）
//        Text s1 = new Text("china");
//        String s2 = new String("china");
        Text s1 = new Text("我爱你中国");
        String s2 = new String("我爱你中国");
        System.out.println("length: text="+s1.getLength()+",string="+s2.length());
        System.out.println("bytes: text="+s1.getBytes().length+",string="+s2.getBytes().length);
//        System.out.println("index: text="+s1.find("a")+",string="+s2.indexOf("a"));
        System.out.println("index: text="+s1.find("中")+",string="+s2.indexOf("中"));
    }
}
