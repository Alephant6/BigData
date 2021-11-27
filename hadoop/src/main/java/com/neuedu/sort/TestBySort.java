package com.neuedu.sort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestBySort {
    public static void main(String[] args) {
        List<Student> studentList = new ArrayList<Student>();
        studentList.add(new Student(2001, "zhangsan", 18));
        studentList.add(new Student(1002, "lishi", 22));
        studentList.add(new Student(1004, "wangwu", 20));
        studentList.add(new Student(1003, "zhangyun", 21));
        // 原始顺序
        System.out.println(studentList);
        // 默认排序
        Collections.sort(studentList);
        System.out.println(studentList);
        // 自定义排序：年龄降序
        studentList.sort(new SortByAgeDESC());
        System.out.println(studentList);
        // 自定义排序：姓名升序
        studentList.sort(new SortByNameASC());
        System.out.println(studentList);
    }
}
