package io.github.s7i.meshtastic.intelligence;

public class Packet {

    public Packet() {

    }
    public Packet(byte[] payload) {
        this.payload = payload;
    }

    byte[] payload;

}
