package com.wuyiccc.bigdata.flinkaction.datastream.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class MyTrigger extends Trigger {
    int count = 0;

    @Override
    public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
        if (count > 9) {
            count = 0;
            System.out.println("触发器触发");
            return TriggerResult.FIRE;
        } else {
            count++;
            System.out.println("onElement : " + element + "Count:" + count);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(Window window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }
}
