package com.jep.flink.tutorial.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream实现Wordcount：读socket（无界流）
 *
 * 需要在本机执行命令，现启动nc，然后启动该程序，在nc界面输入单词
 * 命令 nc -lk 7777 是一个在 Unix 和 Linux 系统中使用 netcat（也称为 nc）工具的常用命令，用于创建一个监听在特定端口（在这个例子中是 7777 端口）的服务器，并接受传入的连接。参数的含义如下：
 *          -l：告诉 netcat 要监听传入的连接，而不是主动发起连接。
 *          -k：允许 netcat 在处理完一个连接后，继续监听以便接受更多的连接。如果不使用 -k 选项，netcat 在处理完一个连接后将自动退出。
 *          7777：指定 netcat 监听的端口号。
 * @author enping.jep
 * @date 2024/10/5 21:22
 **/
public class WordCountStreamUnboundedDemo {
    public static void main(String[] args) throws Exception {
        //  1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA运行时，也可以看到webui，一般用于本地测试
        // 需要引入一个依赖 flink-runtime-web
        // 在idea运行，不指定并行度，默认就是 电脑的 线程数
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //  2. 读取数据： socket   nc -lk 7777  要在本地启动命令
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);

        //  3. 处理数据: 切换、转换、分组、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                                String[] words = value.split(" ");
                                for (String word : words) {
                                    out.collect(Tuple2.of(word, 1));
                                }
                            }
                        }
                )
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1);

        //  4. 输出
        sum.print();

        //  5. 执行
        env.execute();
    }
}
