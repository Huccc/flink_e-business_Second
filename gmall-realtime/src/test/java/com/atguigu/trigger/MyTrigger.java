package com.atguigu.trigger;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

//                                    数据类型       窗口类型
public class MyTrigger extends Trigger<WaterSensor, TimeWindow> {
    @Override
    public TriggerResult onElement(WaterSensor element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            // 事件时间定时器
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // 处理时间定时器
            ctx.registerProcessingTimeTimer(window.maxTimestamp() + 2100L);
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {

        if (time == window.maxTimestamp()) {
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
            return TriggerResult.FIRE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}













