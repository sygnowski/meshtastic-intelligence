package io.github.s7i.meshtastic.intelligence.io;

import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class Packet {

    public Packet() {

    }
    public Packet(byte[] payload) {
        this.payload = payload;
    }

    byte[] payload;

}
