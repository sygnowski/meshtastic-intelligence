package s7i.flink.meshcore;

import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import s7i.flink.meshcore.model.MeshcoreDecoded;

@Slf4j
public class MeshcoreDecodedMapper {

    public static final int PAYLOAD_TEXT = 5;
    private final ObjectMapper objectMapper = new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @SneakyThrows
    public MeshcoreDecoded mapDecoded(byte[] data) {
        return objectMapper.readValue(data, MeshcoreDecoded.class);
    }

    public Optional<MeshcoreDecoded> mapAsOptional(byte[] data) {
        try {
            return Optional.of(mapDecoded(data));
        } catch (Exception e) {
            log.warn("mapping decoded", e);
            return Optional.empty();
        }
    }
}
