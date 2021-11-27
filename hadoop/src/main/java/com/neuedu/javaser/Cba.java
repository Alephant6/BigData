package com.neuedu.javaser;


import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class Cba {
    public static void main(String[] args) {
        Student stu = null;
        try {
            // 序列化
            FileInputStream fs = new FileInputStream("file/s1.txt");
            ObjectInputStream input = new ObjectInputStream(fs);

            stu = (Student)input.readObject();

            input.close();
            fs.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        if (stu != null) {
            System.out.println(stu.getId());
            System.out.println(stu.getName());
            System.out.println(stu.getAge());
        }

    }
}

