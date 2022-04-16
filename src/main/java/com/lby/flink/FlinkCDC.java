package com.lby.flink;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author ：lby
 * @date ：Created at 2022/1/3 15:34
 * @desc ：测试FlinkCDC
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK，如果想要做到断点续传，需要从CheckPoint或者SavePoint启动程序
        // idea属于开发环境，不支持从checkpoint或savepoint恢复任务
        // 2.1 开启checkpoint，每隔5秒做一次ck，指定ck的一致性语义；
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置任务关闭的时候保留最后一次ck数据，默认是 DELETE_ON_CANCELLATION 取消时删除
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.3 指定ck自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        // 2.4 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/flink/flinkCDC"));
        // 2.5 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "liubingyang");

        // 3.创建Flink-MySQL-CDC的source
        Properties confProp = new Properties();
        InputStream dbConfig = FlinkCDC.class.getClassLoader().getResourceAsStream("db.properties");
        confProp.load(dbConfig);
        System.out.println(confProp.toString());
        Properties dbzProp = new Properties();
        dbzProp.setProperty("scan.startup.mode", "initial");
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname(confProp.getProperty("hostname"))
                .port(Integer.valueOf(confProp.getProperty("port")))
                .username(confProp.getProperty("username"))
                .password(confProp.getProperty("password"))
                .databaseList(confProp.getProperty("databaseList"))
                .tableList(confProp.getProperty("tableList"))
                .debeziumProperties(dbzProp)
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        // 4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS = env.addSource(mysqlSource);
        mysqlDS.print();

        // 5.执行任务
        env.execute();
    }
}
