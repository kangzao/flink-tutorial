package com.jep.flink.tutorial.transform;

import com.jep.flink.tutorial.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 过滤
 * @author enping.jep
 * @date 2024/10/6 22:52
 **/
public class FilterDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11), new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3));

        //filter： true保留，false过滤掉
        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return "s1".equals(value.getId());
            }
        });
        filter.print();
        env.execute();
    }
}
