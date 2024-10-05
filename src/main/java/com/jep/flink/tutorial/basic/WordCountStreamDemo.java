package com.jep.flink.tutorial.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream实现Wordcount：读文件（有界流）
 *
 * @author enping.jep
 * @date 2024/10/5 20:56
 **/
public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据:从文件读
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");

        //3.处理数据: 切分、转换、分组、聚合
        //3.1 切分、转换  FlatMapFunction是Flink中的一个函数接口，用于处理输入元素并输出0个或多个元素
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 按照 空格 切分
                String[] words = value.split(" ");
                for (String word : words) {
                    // 转换成 二元组 （word，1） 将每个单词转换为Tuple2类型，其中第一个元素是单词，第二个元素是1
                    Tuple2<String, Integer> wordsAndOne = Tuple2.of(word, 1);
                    // 通过 采集器 向下游发送数据
                    out.collect(wordsAndOne);
                }
            }
        });
        //3.2 分组  分组操作调用的是keyBy方法，可以传入一个匿名函数作为键选择器（KeySelector），指定当前分组的key是什么
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                // 提取Tuple2的第一个元素作为分组的键值
                return value.f0;
            }
        });
        //3.3 聚合  sum方法对Tuple2的第二个元素（即每个单词的计数）进行求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = wordAndOneKS.sum(1);

        //4.输出数据
        sumDS.print();

        //5.执行：类似 sparkstreaming最后 ssc.start()
        env.execute();
    }
}
