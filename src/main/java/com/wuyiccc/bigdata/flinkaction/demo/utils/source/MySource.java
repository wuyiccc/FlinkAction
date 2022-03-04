package com.wuyiccc.bigdata.flinkaction.demo.utils.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author wuyiccc
 * @date 2021/12/8 22:48
 */
public class MySource implements SourceFunction<String> {

    private boolean isRunning = true;


    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        List<String> stringList = new ArrayList<>();
        stringList.add("word");
        stringList.add("flink");
        stringList.add("steam");
        stringList.add("batch");
        stringList.add("table");
        stringList.add("sql");
        stringList.add("hello");
        int size = stringList.size();
        while (isRunning) {
            int i = new Random().nextInt(size);
            ctx.collect(stringList.get(i));
            System.out.println("Source:"+stringList.get(i));
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
