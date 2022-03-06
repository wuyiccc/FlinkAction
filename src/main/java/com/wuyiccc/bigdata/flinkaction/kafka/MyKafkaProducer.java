package com.wuyiccc.bigdata.flinkaction.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
/*https://www.cnblogs.com/importbigdata/p/10779930.html*/
import java.util.Properties;

public class MyKafkaProducer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.addSource(new MySource());

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("test", new SimpleStringSchema(), properties);

        //附加到每个记录的（事件时间）时间戳记写入Kafka
        producer.setWriteTimestampToKafka(true);


        dataStreamSource.addSink(producer);
        env.execute();
    }
}
