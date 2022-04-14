package com.neuedu.phoenix;

import java.sql.*;

public class JdbcDemo {
    public static void main(String[] args) {
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            // 加载Phoenix JDBC驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // 创建Connection对象
            connection = DriverManager.getConnection("jdbc:phoenix:master:2181");
            // 定义SQL语句
            String sql = "SELECT DID,DNAME,REMARK FROM DEPT";
            // 创建PreparedStatement对象
            stmt = connection.prepareStatement(sql);
            // 执行sql，返回结果集
            rs = stmt.executeQuery();
            // 打印标题栏
            System.out.println("did\tdname\tremark");
            // 遍历结果集中的每一行
            while (rs.next()) {
                // 读取该行中的每一列
                System.out.println(rs.getInt(1) + "\t" + rs.getString(2)
                        + rs.getString(3));
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }finally {
            // 释放资源
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}
