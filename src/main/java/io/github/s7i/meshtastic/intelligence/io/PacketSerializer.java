package io.github.s7i.meshtastic.intelligence.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PacketSerializer extends Serializer<Packet> {

    @Override
    public void write(Kryo kryo, Output output, Packet packet) {
        output.writeLong(packet.timestamp, true);
        output.writeInt(packet.payload.length, true);
        output.writeBytes(packet.payload);
    }

    @Override
    public Packet read(Kryo kryo, Input input, Class<Packet> type) {
        Packet packet = new Packet();

        packet.timestamp = input.readLong(true);

        int len = input.readInt(true);
        byte[] pyloadBuffer = new byte[len];
        int readBytes = input.read(pyloadBuffer);

        if (readBytes != len) {
            log.warn("mismatch in declared length: {} <> {}", len, readBytes);
        }

        packet.payload = pyloadBuffer;
        return packet;
    }
}
