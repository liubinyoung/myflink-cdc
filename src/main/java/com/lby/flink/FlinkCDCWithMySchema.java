package com.lby.flink;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.lby.flink.utils.PropertiesUtils;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import java.util.Map;
import java.util.Properties;

/**
 * @author ：lby
 * @date ：Created at 2022/1/3 21:02
 * @desc ：测试FlinkCDC，使用自定义反序列化器
 */
public class FlinkCDCWithMySchema {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/flink/FlinkCDC"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));
        System.setProperty("HADOOP_USER_NAME", "liubingyang");

        Properties dbzProp = new Properties();
        dbzProp.setProperty("scan.startup.mode", "initial");
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname(PropertiesUtils.getDBProperty("hostname"))
                .port(Integer.valueOf(PropertiesUtils.getDBProperty("port")))
                .username(PropertiesUtils.getDBProperty("username"))
                .password(PropertiesUtils.getDBProperty("password"))
                .databaseList(PropertiesUtils.getDBProperty("databaseList"))
                .tableList(PropertiesUtils.getDBProperty("tableList"))
                .debeziumProperties(dbzProp)
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        // {"database":"gmall-2020-04","table":"z_user_info","type":"insert","ts":1589385314,"xid":82982,"xoffset":0,"data":{"id":30,"user_name":"zhang3","tel":"13810001010"}}
                        String topic = sourceRecord.topic();
                        String[] topicSplit = topic.split("\\.");
                        String database = topicSplit[1];
                        String table = topicSplit[2];

                        Struct value = (Struct) sourceRecord.value();
                        // 操作类型
                        // String op = value.getString("op");
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        String type = operation.toString().toLowerCase();
                        // 时间戳
                        long ts_ms = value.getInt64("ts_ms");

                        // data封装
                        JSONObject data = new JSONObject();
                        if ("create".equals(type) || "update".equals(type)) {
                            Struct after = value.getStruct("after");
                            for (Field field : after.schema().fields()) {
                                Object o = after.get(field);
                                data.put(field.name(), o);
                            }
                        } else if ("delete".equals(type)) {
                            Struct before = value.getStruct("before");
                            for (Field field : before.schema().fields()) {
                                Object o = before.get(field);
                                data.put(field.name(), o);
                            }
                        }

                        // offset
                        Map<String, ?> offsetMap = sourceRecord.sourceOffset();
                        Object offset = offsetMap.get("pos");

                        JSONObject result = new JSONObject();
                        result.put("database", database);
                        result.put("table", table);
                        result.put("type", type);
                        result.put("offset", offset);
                        result.put("ts", ts_ms);
                        result.put("data", data);
                        collector.collect(result.toJSONString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .build();

        DataStreamSource<String> mysqlSourceDS = env.addSource(mysqlSource);
        mysqlSourceDS.print();

        env.execute();
    }
}
