package s7i.flink.io;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class Packet {

    public Packet() {

    }

    long timestamp;
    byte[] key;
    byte[] payload;
}
