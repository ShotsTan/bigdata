package util;

import constant.Constant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @ClassName: FlinkSourceUtil
 * @Description: TODO 类描述
 * @Author: Tanjh
 * @Date: 2024/03/12 10:59
 * @Company: Copyright©
 **/

public class FlinkSourceUtil {
    //KafkaSource
    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        return KafkaSource.<String>builder().setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        return new String(message, StandardCharsets.UTF_8);
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                }).build();


    }


    //KafkaSink
    public static KafkaSink<String> getKafkaSink(String groupId,String topic){
        return KafkaSink.<String>builder()
                .build();

    }



}
