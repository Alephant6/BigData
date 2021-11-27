package com.neuedu.javaser;


import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

public class Abc {
    public static void main(String[] args) {
        // 实例化对象
        Student stu = new Student(1001, "张三", 21);
        try {
            // 序列化
            FileOutputStream fs = new FileOutputStream("file/s1.txt");
            ObjectOutputStream out = new ObjectOutputStream(fs);

            out.writeObject(stu);

            out.close();
            fs.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

