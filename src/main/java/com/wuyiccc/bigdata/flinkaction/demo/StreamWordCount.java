package com.wuyiccc.bigdata.flinkaction.demo;

import com.wuyiccc.bigdata.utils.Splitter;
import com.wuyiccc.bigdata.utils.source.MySource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wuyiccc
 * @date 2021/12/8 22:59
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 获取流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = env.addSource(new MySource()).flatMap(new Splitter()).keyBy(0).sum(1);
        result.print();
        // 流式数据时, flink是懒加载的, 所以此时必须调用execute方法才可以执行
        env.execute();

    }
}
