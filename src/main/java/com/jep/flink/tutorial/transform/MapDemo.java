package com.jep.flink.tutorial.transform;

import com.jep.flink.tutorial.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author enping.jep
 * @date 2024/10/6 22:46
 **/
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2), new WaterSensor("s3", 3L, 3));

        // map算子： 一进一出

        // 方式一： 匿名实现类  输入是WaterSensor 输出是String
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
//            @Override
//            public String map(WaterSensor value) throws Exception {
//                return value.getId();
//            }
//        });

        // 方式二： lambda表达式
//        SingleOutputStreamOperator<String> map = sensorDS.map(sensor -> sensor.getId());

        // 方式三： 定义一个类来实现MapFunction
//        SingleOutputStreamOperator<String> map = sensorDS.map(new MyMapFunction());
        SingleOutputStreamOperator<String> map = sensorDS.map(new MyMapFunction());
        map.print();


        env.execute();
    }

    public static class MyMapFunction implements MapFunction<WaterSensor, String> {

        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
    }

}
