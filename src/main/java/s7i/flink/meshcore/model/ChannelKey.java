package s7i.flink.meshcore.model;

import java.io.Serializable;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import s7i.flink.meshcore.model.MeshcoreDecoded.Decoded;
import s7i.flink.meshcore.model.MeshcoreDecoded.Payload;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChannelKey implements Serializable {

    public static ChannelKey from(MeshcoreDecoded meshcoreDecoded) {
        var hash = Optional.ofNullable(meshcoreDecoded)
              .map(MeshcoreDecoded::getPayload)
              .map(Payload::getDecoded)
              .map(Decoded::getChannelHash).
              orElse("#unknown#");
        return new ChannelKey(hash);
    }


    private String hash;
}
