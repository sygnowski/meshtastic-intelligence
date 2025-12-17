package s7i.flink.meshcore;

import static java.util.Optional.ofNullable;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import s7i.flink.io.Packet;
import s7i.flink.meshcore.MeshcoreDecoded.Decoded;
import s7i.flink.meshcore.MeshcoreDecoded.Payload;

@Slf4j
public class ProcessDecoded extends ProcessFunction<Packet, TextMessage> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        objectMapper = new ObjectMapper();
    }

    @Override
    public void processElement(Packet packet, ProcessFunction<Packet, TextMessage>.Context context, Collector<TextMessage> collector) throws Exception {
        try {
            var decoded = objectMapper.readValue(packet.payload(), MeshcoreDecoded.class);
            ofNullable(decoded.getPayload())
                  .map(Payload::getDecoded)
                  .map(Decoded::getDecrypted)
                  .ifPresent( decrypted ->
                        collector.collect( new TextMessage(
                              decrypted.getSender(), decrypted.getMessage()
                  )));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
