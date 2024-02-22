import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import reactor.util.function.Tuple2;
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
        conf.setInteger("rest.port",12000);

        // 1.执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        //2.定义Source获取数据
        DataStreamSource<String> sourceStream = env.socketTextStream("hadoop162", 9999);

        //3.定义Sink
        KafkaSink<Tuple2<String,Integer>> kafkaSink = KafkaSink.<Tuple2<String,Integer>>builder()
                .setBootstrapServers("hadoop162")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("flink")
                        .setValueSerializationSchema((SerializationSchema<Tuple2<String, Integer>>) objects -> objects.toString().getBytes())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();


        //4.处理数据并输出，发送kafka
        sourceStream.flatMap(new RichFlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
//                    collector.collect();
                    collector.close();
                }

            }
        }).sinkTo(kafkaSink);




    }
}
