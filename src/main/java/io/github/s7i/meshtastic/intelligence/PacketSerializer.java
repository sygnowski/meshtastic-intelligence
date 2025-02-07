package io.github.s7i.meshtastic.intelligence;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PacketSerializer extends Serializer<Packet> {

    @Override
    public void write(Kryo kryo, Output output, Packet packet) {
        output.writeInt(packet.payload.length, true);
        output.writeBytes(packet.payload);
    }

    @Override
    public Packet read(Kryo kryo, Input input, Class<Packet> type) {
        Packet packet = new Packet();

        int len = input.readInt(true);
        byte[] paylod = new byte[len];
        input.read(paylod);

        packet.payload = paylod;
        return packet;
    }
}
