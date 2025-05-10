package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.Constants;
import com.geeksville.mesh.MeshProtos.Data;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import com.geeksville.mesh.MeshProtos.ToRadio;
import com.geeksville.mesh.Portnums.PortNum;
import com.google.protobuf.ByteString;
import io.github.s7i.meshtastic.intelligence.Model.SendMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SendMessageKafkaSerializer implements KafkaRecordSerializationSchema<SendMessage> {


    private final String sendTopic;
    private final String sendKey;
    transient PacketIdGenerator packetIdGenerator;

    final int fromNode = (int) (4072996141L & 0xffffffffL);

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

        var payload = ByteString.copyFromUtf8(sendMessage.getMesssage());

        if (payload.size() > Constants.DATA_PAYLOAD_LEN_VALUE) {
            throw new IllegalStateException("payload too big");
        }

        var rx = ToRadio.newBuilder()
              .setPacket(MeshPacket.newBuilder()
                    .setId(packetIdGenerator.generatePacketId())
                    .setFrom(fromNode)
                    .setTo(sendMessage.getToNode())
                    .setRxTime((int) Instant.now().getEpochSecond())
                    .setChannel(sendMessage.getChannel())
                    .setDecoded(Data.newBuilder()
                          .setPayload(payload)
                          .setPortnum(PortNum.TEXT_MESSAGE_APP)))
              .build();

        return new ProducerRecord<>(sendTopic, sendKey.getBytes(StandardCharsets.UTF_8), rx.toByteArray());
    }

}
