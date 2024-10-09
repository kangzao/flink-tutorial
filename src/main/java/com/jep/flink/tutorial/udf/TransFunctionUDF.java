package com.jep.flink.tutorial.udf;

import com.jep.flink.tutorial.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义函数
 *
 * @author enping.jep
 * @date 2024/10/7 17:00
 **/
public class TransFunctionUDF {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(

                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        SingleOutputStreamOperator<WaterSensor> filter = stream.filter(new UserFilter("sensor_1"));
        filter.print();
        env.execute();
    }

    //自定义函数
    public static class UserFilter implements FilterFunction<WaterSensor> {

        private String filterStr;

        public UserFilter(String filterStr) {
            this.filterStr = filterStr;
        }

        @Override
        public boolean filter(WaterSensor e) throws Exception {
            return filterStr.equals(e.getId());
        }
    }

}
