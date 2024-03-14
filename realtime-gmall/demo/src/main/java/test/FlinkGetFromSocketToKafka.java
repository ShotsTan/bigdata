import constant.Constant;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import redis.clients.jedis.Tuple;

import java.util.Iterator;

/**
 * @ClassName: FlinkGetFromSocketToKafka
 * @Description: 测试，从Socket获取数据，进行wordCount后提交到kafka
 * @Author: Tanjh
 * @Date: 2024/02/22 14:25
 * @Company: Copyright©
 **/

public class FlinkGetFromSocketToKafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 12000);

        // 1.执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        //2.定义Source获取数据
        DataStreamSource<String> sourceStream = env.socketTextStream("hadoop162", 9999);

        //3.定义Sink
        KafkaSink<Tuple2<String, Integer>> kafkaSink = KafkaSink.<Tuple2<String, Integer>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(Constant.Topic_Flink_Test00)
                        .setValueSerializationSchema((SerializationSchema<Tuple2<String, Integer>>) objects -> objects.toString().getBytes())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix("tanjh-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();


        //4.处理数据并输出，发送kafka
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = sourceStream
                .flatMap(new MyFlatMap()).keyBy(x -> x.f0)
                .sum(1);

        sum.print();
        sum.sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : line.split(" ")) {
                collector.collect(Tuple2.of(word, 1));
            }

        }
    }
}
