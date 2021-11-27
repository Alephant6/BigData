package com.neuedu.sort;

import java.io.Serializable;

/**
 * 实体类Student，实现序列化接口Serializable, Comparable
 *
 * @author alephant
 */
public class Student implements Serializable, Comparable {
    private Integer id;
    private String name;
    private Integer age;

    public Student() {
    }

    public Student(Integer id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "学号：" + this.id + ",姓名：" + this.name + ",年龄：" + this.age;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public int compareTo(Object o) {
        // 返回值有要求：>0是前大于后，=0相等，<0前小于后
        Student other = (Student) o;
        if (other == null) {
            return 1;
        }
        // 所有的比较均会最终使用基本类型
//        if (this.id > other.id) {
//            return 1;
//        } else if (this.id == other.id) {
//            return 0;
//        } else {
//            return -1;
//        }
//        return Integer.compare(this.id, other.id);//升序
        return -Integer.compare(this.id, other.id);//降序
    }
}
