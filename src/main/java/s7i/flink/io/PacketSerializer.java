package s7i.flink.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PacketSerializer extends Serializer<Packet> {

    public static final int NULL_VALUE = -1;

    @Override
    public void write(Kryo kryo, Output output, Packet packet) {
        output.writeLong(packet.timestamp, true);

        int lenPayload = packet.payload != null ? packet.payload.length : NULL_VALUE;

        output.writeInt(lenPayload, true);
        if (packet.payload != null) {
            output.writeBytes(packet.payload);
        }

        int lenKey = packet.key != null ? packet.key.length : NULL_VALUE;

        output.writeInt(lenKey, true);
        if (packet.key != null) {
            output.writeBytes(packet.key);
        }
    }

    @Override
    public Packet read(Kryo kryo, Input input, Class<Packet> type) {
        int len;

        Packet packet = new Packet();

        packet.timestamp = input.readLong(true);

        len = input.readInt(true);

        if (len != NULL_VALUE) {
            byte[] pyloadBuffer = new byte[len];
            int readBytes = input.read(pyloadBuffer);

            if (readBytes != len) {
                log.warn("mismatch in declared length: {} <> {}", len, readBytes);
            }
            packet.payload = pyloadBuffer;
        }

        len = input.readInt(true);
        if (len != NULL_VALUE) {
            byte[] keyBuffer = new byte[len];
            int readBytes = input.read(keyBuffer);

            if (readBytes != len) {
                log.warn("mismatch in declared length: {} <> {}", len, readBytes);
            }
            packet.key = keyBuffer;
        }

        return packet;
    }
}
