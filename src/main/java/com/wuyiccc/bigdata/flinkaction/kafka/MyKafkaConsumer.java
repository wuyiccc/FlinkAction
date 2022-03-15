package com.wuyiccc.bigdata.flinkaction.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;


public class MyKafkaConsumer {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        consumer.setStartFromEarliest();//从最早的数据开始进行消费，忽略存储的offset信息
       /* consumer.setStartFromEarliest();     // 尽可能从最早的记录开始
        consumer.setStartFromLatest();       // 从最新的记录开始
        consumer.setStartFromTimestamp(...); // 从指定的时间开始（毫秒）
        consumer.setStartFromGroupOffsets(); // 默认的方法*/
        DataStream<String> stream = env
                .addSource(consumer);

        //1
        stream.print();
        env.execute();

    }

}
