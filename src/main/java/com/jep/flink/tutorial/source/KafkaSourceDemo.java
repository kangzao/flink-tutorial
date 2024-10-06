package com.jep.flink.tutorial.source;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author enping.jep
 * @date 2024/10/6 19:11
 **/
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从Kafka读： 新Source架构
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("127.0.0.1:9092") // 指定kafka节点的地址和端口
                .setGroupId("test")  // 指定消费者组的id
                .setTopics("first-topic")   // 指定消费的 Topic
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 指定 反序列化器，这个是反序列化value
                .setStartingOffsets(OffsetsInitializer.latest())  // flink消费kafka的策略 如果有offset，就从offset消费，否则从最新消费
                .build();


        env.fromSource(kafkaSource, WatermarkStrategy.
                forBoundedOutOfOrderness(Duration.ofSeconds(3)), "kafkasource").print();


        env.execute();
    }
}
