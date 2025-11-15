package io.github.s7i.meshtastic.intelligence;

import io.github.s7i.meshtastic.intelligence.Model.SendMessage;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SendMessageKafkaSerializer implements KafkaRecordSerializationSchema<SendMessage>, TextToRadioMapper {


    private final String sendTopic;
    private final String sendKey;
    private transient PacketIdGenerator packetIdGenerator;

//    final int fromNode = (int) (4072996141L & 0xffffffffL);

    public SendMessageKafkaSerializer(String sendTopic, String sendKey) {
        this.sendTopic = sendTopic;
        this.sendKey = sendKey;
    }

    @Override
    public void open(InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        packetIdGenerator = new PacketIdGenerator();
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(SendMessage sendMessage, KafkaSinkContext kafkaSinkContext, Long aLong) {

        return new ProducerRecord<>(
              sendTopic,
              sendKey.getBytes(StandardCharsets.UTF_8),
              toToRadioData(sendMessage)
        );
    }

    @Override
    public PacketIdGenerator packetIdGenerator() {
        return packetIdGenerator;
    }

}
