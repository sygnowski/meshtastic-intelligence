package io.github.s7i.meshtastic.intelligence.io;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(fluent = true)
public class Packet {

    public Packet() {

    }
    public Packet(byte[] payload) {
        this.payload = payload;
    }

    byte[] payload;
    long timestamp;
}
