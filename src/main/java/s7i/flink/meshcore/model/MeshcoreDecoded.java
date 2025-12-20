package s7i.flink.meshcore.model;

import java.io.Serializable;
import java.util.List;
import lombok.Data;

@Data
public class MeshcoreDecoded implements Serializable {

    @Data
    public static class Decoded implements Serializable {
        Integer type;
        String channelHash;
        Decrypted decrypted;
    }

    @Data
    public static class Decrypted implements Serializable {
        Long timestamp;
        String sender;
        String message;
    }

    @Data
    public static class Payload implements Serializable {
        Decoded decoded;
    }
    private Integer payloadType;
    private List<String> path;
    private Payload payload;
}
