package com.lby.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author ：lby
 * @date ：Created at 2022/1/3 20:35
 * @desc ：测试flinksql-cdc
 */
public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.创建Flink-MySQL-CDC的Source
        tableEnv.executeSql("CREATE TABLE base_sale_attr(" +
                "id bigint," +
                "name string" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'gmall0820'," +
                "'table-name' = 'base_sale_attr'" +
                ")");

        // 3.选择同步数据
        TableResult tableResult = tableEnv.executeSql("select * from base_sale_attr");
        tableResult.print();

        // 4.执行
        env.execute();
    }
}
