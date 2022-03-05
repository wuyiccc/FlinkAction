package com.wuyiccc.bigdata.flinkaction.datastream.eventtime;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * 自定义事件时间和水位线
 */
public class WindowAndWatermarkDemo {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用EventTime，默认使用processtime
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1,默认并行度是当前机器的CPU数量
        sEnv.setParallelism(1);


        DataStream<String> input = sEnv.addSource(new MySource());
        /*消息格式：String,time。例如：消息1,1599456459000*/
        //解析输入的数据
        DataStream<Tuple2<String, Long>> inputMap = input.map(new MapFunction<String, Tuple2<String, Long>>() {

            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });
        //抽取timestamp，生成Watermark，这里使用的是Flink1.11最新版的水印生成策略
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {

            @Override
            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxTimestamp;
                    private long delay = 3000;

                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                        maxTimestamp = Math.max(maxTimestamp, event.f1);

                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp - delay));
                    }
                };
            }
        });
        /*获取水印信息*/
        waterMarkStream.process(new ProcessFunction<Tuple2<String, Long>, Object>() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Object> out) throws Exception {
                long w = ctx.timerService().currentWatermark();
                System.out.println(" 水印 ： " + w + "水印时间" + sdf.format(w) + "消息的事件时间" +
                        sdf.format(value.f1));
            }
        });
        waterMarkStream.print();
        sEnv.execute();
    }
}
