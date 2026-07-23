package s7i.flink.meshcore;

import static java.util.Optional.ofNullable;
import static s7i.flink.meshcore.MeshcoreDecodedMapper.PAYLOAD_TEXT;

import java.util.Optional;

import org.slf4j.LoggerFactory;
import s7i.flink.meshcore.model.MeshcoreDecoded;
import s7i.flink.meshcore.model.MeshcoreDecoded.Decoded;
import s7i.flink.meshcore.model.MeshcoreDecoded.Payload;
import s7i.flink.meshcore.model.TextMessage;


public interface TextMessageMapper {

    default Optional<TextMessage> map(MeshcoreDecoded decoded) {
        try {
            if (decoded.getPayloadType() != PAYLOAD_TEXT) {
                return Optional.empty();
            }
            return ofNullable(decoded.getPayload())
                    .map(Payload::getDecoded)
                    .map(Decoded::getDecrypted)
                    .map(decrypted -> TextMessage.builder()
                            .messageHash(decoded.getMessageHash())
                            .timestamp(decrypted.getTimestamp())
                            .channelHash(decoded.getPayload().getDecoded().getChannelHash())
                            .from(decrypted.getSender())
                            .text(decrypted.getMessage())
                            .build()
                            .addPath(decoded.getPath())
                    );
        } catch (Exception e) {
            LoggerFactory
                    .getLogger(TextMessageMapper.class)
                    .warn("Parsing issue", e);
            return Optional.empty();
        }
    }
}
