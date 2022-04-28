package com.neuedu.brazil;

import netscape.javascript.JSObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * 数据清洗与数据导入类：数据清洗，数据导入hbase表中
 *
 * @author Alephant
 */
public class Step1 {
    private static class ImportMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //Estacao;Data;Hora;Precipitacao;TempBulboSeco;TempBulboUmido;TempMaxima;TempMinima;UmidadeRelativa;PressaoAtmEstacao;PressaoAtmMar;DirecaoVento;VelocidadeVento;Insolacao;Nebulosidade;Evaporacao Piche;Temp Comp Media;Umidade Relativa Media;Velocidade do Vento Media;
            //82024;01/01/1961;0000;;;;32.3;;;;;;;4.4;;;26.56;82.5;3;
            //82024;01/01/1961;1200;;26;23.9;;22.9;83;994.2;;5;5;;8;;;;;
            //82024;01/01/1961;1800;;32.3;27;;;65;991.6;;5;3;;9;;;;;
            String line = value.toString();
            // 判断内容是否为null、空字符串、空格组成的字符串
            if (StringUtils.isBlank(line)) {
                return;
            }
            // 跳过标题栏
            if (line.startsWith("Estacao")) {
                return;
            }
            // 只取巴西利亚83377气象数据
            if (!line.startsWith("83377")) {
                return;
            }
            // 拆分数据
            String[] items = line.split(";", 19);
            // 输出
            // <站点编号_日期,降雨量_最高温度_最低温度_平均温度>
            context.write(new Text(items[0] + "_" + items[1]),
                    new Text(items[3] + "_" + items[6] + "_" + items[7] + "_" + items[16]));
        }
    }

    private static class ImportReducer extends TableReducer<Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
            //接收的数据格式
            // <站点编号_日期,<降雨量_最高温度_最低温度_平均温度,降雨量_最高温度_最低温度_平均温度,降雨量_最高温度_最低温度_平均温度>>
            // 拆分：站点编号_日期
            String[] items1 = key.toString().split("_");
            String stattion_code = items1[0];
            String date = items1[1];
            Float precipitation = 0.0F;
            Float maxTemperature = 0.0F;
            Float minTemperature = Float.MAX_VALUE;
            Float avgTemperature = 0.0F;
            // 三行合一
            for (Text v : values) {
                String[] items2 = v.toString().split("_", 4);
                // 降雨量3个时间累加
                precipitation +=
                        Float.parseFloat(StringUtils.isBlank(items2[0]) ? "0" : items2[0]);
                // 最高温度
                maxTemperature = Math.max(maxTemperature,
                        Float.parseFloat(StringUtils.isBlank(items2[1]) ? "0" : items2[1]));
                // 最低温度
                minTemperature = Math.min(maxTemperature,
                        Float.parseFloat(StringUtils.isBlank(items2[2]) ? "" + Float.MAX_VALUE : items2[2]));
                // 平均温度
                avgTemperature = Math.max(avgTemperature,
                        Float.parseFloat(StringUtils.isBlank(items2[3]) ? "0" : items2[3]));
            }
            // 定义列簇、列、值
            byte[] family = Bytes.toBytes("info");
            byte[] rowKey = Bytes.toBytes(key.toString());
            byte[] c1 = Bytes.toBytes("pecipitation");
            byte[] c2 = Bytes.toBytes("maxtemperature");
            byte[] c3= Bytes.toBytes("mintemperature");
            byte[] c4 = Bytes.toBytes("avgtemperature");
            byte[] v1 = Bytes.toBytes(precipitation);
            byte[] v2 = Bytes.toBytes(maxTemperature);
            byte[] v3 = Bytes.toBytes(minTemperature);
            byte[] v4 = Bytes.toBytes(avgTemperature);
            // 构建Put对象
            Put put = new Put(rowKey);
            put.addColumn(family, c1, v1);
            put.addColumn(family, c2, v2);
            put.addColumn(family, c3, v3);
            put.addColumn(family, c4, v4);
            // 输出
            context.write(NullWritable.get(),put);
        }
    }

    public static void run() {
        try {
            // 定义输入路径
            String input = "hdfs://master:9000/weather_data";
            // 定义表名
            String tableName = "weather";
            // 定义写入的表
            Configuration conf = HbaseUtils.getCONF();
            conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
            // 创建表
            HbaseUtils.createTable(tableName, "info", true);
            // 创建Job对象
            Job job = Job.getInstance(conf, "import weather data");
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            // 设置mapper
            job.setMapperClass(ImportMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // 设置reducer
            job.setReducerClass(ImportReducer.class);
            // 设置输出
            job.setOutputFormatClass(TableOutputFormat.class);
            // 执行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("巴西数据中巴西利亚数据导入成功~~~");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
