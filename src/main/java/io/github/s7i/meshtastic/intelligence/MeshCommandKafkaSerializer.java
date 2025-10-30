package io.github.s7i.meshtastic.intelligence;

import io.github.s7i.meshtastic.intelligence.Model.MeshCommand;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

@RequiredArgsConstructor
@Slf4j
public class MeshCommandKafkaSerializer implements KafkaRecordSerializationSchema<MeshCommand> {


    private transient ObjectMapper objectMapper;
    private final String topic;

    @Override
    public void open(InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        objectMapper = new ObjectMapper();
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(MeshCommand element, KafkaSinkContext context, Long timestamp) {
        byte[] data = null;

        try {
            data = objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            log.error("oops", e);
            throw new RuntimeException(e);
        }

        return new ProducerRecord<>(topic, data);
    }
}
