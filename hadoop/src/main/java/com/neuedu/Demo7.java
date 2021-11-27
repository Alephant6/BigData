package com.neuedu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 其它操作：删除、修改、查看块
 * @author Alephant
 */
public class Demo7 {
    public static void main(String[] args) {
        // 1-构建配置对象
        Configuration conf = new Configuration();
        // 2-配置hadoop集群属性
        conf.set("fs.defaultFS","hdfs://master:9000");
        try{
            // 3-获取HDFS对象
            FileSystem fs = FileSystem.get(conf);
            // 定义目标文件：HDFS中文件的路径,/是根目录
            Path dst = new Path("/f.txt");
            // 查看块
            FileStatus s = fs.getFileLinkStatus(dst);
            BlockLocation[] blocks = fs.getFileBlockLocations(s,0,s.getLen());
            for (BlockLocation b : blocks) {
                for (int i = 0; i < b.getNames().length; i++) {
                    System.out.println(b.getNames()[i]);
                    System.out.println(b.getOffset());
                    System.out.println(b.getLength());
                    System.out.println(b.getHosts()[i]);
                }
            }
            // 修改文件名
            // 判断文件存在
            if (!fs.exists(dst)) {
                System.out.println("文件不存在");
                return;
            }
            boolean flag = fs.rename(dst, new Path("/fff.txt"));
            if (flag) {
                System.out.println("文件名修改成功");
            }
            // 删除文件
            // 判断文件存在
            if (!fs.exists(new Path("/fff.txt"))) {
                System.out.println("文件不存在");
                return;
            }
            // flag = fs.delete(new Path("/fff.txt"));
            flag = fs.delete(new Path("/fff.txt"),true);
            if (flag) {
                System.out.println("文件删除成功");
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
