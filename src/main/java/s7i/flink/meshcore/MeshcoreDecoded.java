package s7i.flink.meshcore;

import java.util.List;
import lombok.Data;

@Data
public class MeshcoreDecoded {

    @Data
    public static class Decoded {
        Integer type;
        String channelHash;
        Decrypted decrypted;
    }

    @Data
    public static class Decrypted {
        Long timestamp;
        String sender;
        String message;
    }

    @Data
    public static class Payload {
        Decoded decoded;
    }
    private Integer payloadType;
    private List<String> path;
    private Payload payload;
}
