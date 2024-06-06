package com.my.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlReadKafkaExample {
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 根据实际情况设置并行度
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 创建一个表来表示Kafka源，这里假设消息体为JSON格式且包含字段"text"
        String kafkaSourceDDL = "CREATE TABLE kafkaSource (\n" +
                "  id STRING,\n" +
                "  amount int\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'flink-sql-test',\n" +
                "  'properties.bootstrap.servers' = '192.168.50.185:9093,192.168.50.185:9094,192.168.50.185:9095',\n" +
                "  'format' = 'json',\n" +
                "  'properties.group.id' = 'myGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset' \n" +
                ")";

        // 执行DDL创建表
        tEnv.executeSql(kafkaSourceDDL);

        // 查询Kafka数据并打印到控制台
        String query = "SELECT id,amount FROM kafkaSource";
        tEnv.sqlQuery(query).execute().print();

        // 启动Flink作业
        env.execute("Flink SQL Read from Kafka");
    }
}
