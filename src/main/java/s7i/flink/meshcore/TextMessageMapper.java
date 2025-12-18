package s7i.flink.meshcore;

import static java.util.Optional.ofNullable;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import s7i.flink.io.Packet;
import s7i.flink.meshcore.MeshcoreDecoded.Decoded;
import s7i.flink.meshcore.MeshcoreDecoded.Decrypted;
import s7i.flink.meshcore.MeshcoreDecoded.Payload;

@Slf4j
public class TextMessageMapper {

    public static final int PAYLOAD_TEXT = 5;
    private final ObjectMapper objectMapper = new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public Optional<TextMessage> map(Packet packet) {
        try {
            final var decoded = objectMapper.readValue(packet.payload(), MeshcoreDecoded.class);
            if (decoded.getPayloadType() != PAYLOAD_TEXT) {
                return Optional.empty();
            }
            return ofNullable(decoded.getPayload())
                  .map(Payload::getDecoded)
                  .map(Decoded::getDecrypted)
                  .map(decrypted -> new TextMessage(
                              title(decoded, decrypted), message(decoded, decrypted)
                        )
                  );
        } catch (Exception e) {
            log.warn("Parsing issue", e);
            return Optional.empty();
        }
    }

    static String title(MeshcoreDecoded decoded, Decrypted decrypted) {
        return decrypted.getSender()
              + " on channel: "
              + decoded.getPayload().getDecoded().getChannelHash();
    }

    static String message(MeshcoreDecoded decoded, Decrypted decrypted) {
        return "Path: "
              + String.join("->", decoded.getPath())
              + "\n" + "\n"
              + decrypted.getMessage();
    }
}
