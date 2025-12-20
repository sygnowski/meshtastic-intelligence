package s7i.flink.meshcore;

import static java.util.Optional.ofNullable;
import static s7i.flink.meshcore.MeshcoreDecodedMapper.PAYLOAD_TEXT;

import java.time.ZonedDateTime;
import java.util.Optional;

import org.slf4j.LoggerFactory;
import s7i.flink.Utils;
import s7i.flink.meshcore.model.MeshcoreDecoded;
import s7i.flink.meshcore.model.MeshcoreDecoded.Decoded;
import s7i.flink.meshcore.model.MeshcoreDecoded.Decrypted;
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
                            .timestamp(decrypted.getTimestamp())
                            .channelHash(decoded.getPayload().getDecoded().getChannelHash())
                            .from(title(decoded, decrypted))
                            .message(message(decoded, decrypted))
                            .build()
                    );
        } catch (Exception e) {
            LoggerFactory
                    .getLogger(TextMessageMapper.class)
                    .warn("Parsing issue", e);
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
                + String.join("->", decoded.getPath()) + "\n"
                + "Time: " + Utils.fromEpoch(decrypted.getTimestamp())
                + "\n"
                + decrypted.getMessage();
    }
}
