package io.github.s7i.meshtastic.intelligence.io;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PacketTest {


    private Kryo kryo;

    @BeforeEach
    public void setupKryo() {
        kryo = new Kryo();
        kryo.register(Packet.class, new PacketSerializer());
    }


    @Test
    void testSerde() throws IOException {

        var payload = new byte[]{
              10, 12, 14, 12, 23, 74
        };
        long timestamp = System.currentTimeMillis();

        var kryo = new Kryo();

        Packet packet = new Packet();
        packet.timestamp(timestamp);
        packet.payload(payload);

        byte[] out;
        var baos = new ByteArrayOutputStream(50);
        try (var output = new Output(baos)) {
            kryo.writeObject(output, packet);

            output.flush();
        }
        out = baos.toByteArray();

        var restored = kryo.readObject(new Input(out), Packet.class);

        assertEquals(timestamp, restored.timestamp());
        assertArrayEquals(payload, restored.payload());
        assertNull(restored.key());
    }

    @Test
    void testSerdeWithKey() throws IOException {

        var payload = new byte[]{
              10, 12, 14, 12, 23, 74
        };
        long timestamp = System.currentTimeMillis();
        String wantedKey = "my-key" + System.currentTimeMillis();

        Packet packet = new Packet();
        packet.timestamp(timestamp);
        packet.payload(payload);
        packet.key(wantedKey.getBytes(StandardCharsets.UTF_8));

        byte[] out;
        var baos = new ByteArrayOutputStream(50);
        try (var output = new Output(baos)) {
            kryo.writeObject(output, packet);

            output.flush();
        }
        out = baos.toByteArray();

        var restored = kryo.readObject(new Input(out), Packet.class);

        assertEquals(timestamp, restored.timestamp());
        assertArrayEquals(payload, restored.payload());
        assertEquals(wantedKey, new String(restored.key()));
    }

    @Test
    void testSerdeNoPayload() throws IOException {

        long timestamp = System.currentTimeMillis();
        String wantedKey = "my-key" + System.currentTimeMillis();

        Packet packet = new Packet();
        packet.timestamp(timestamp);
        packet.key(wantedKey.getBytes(StandardCharsets.UTF_8));

        byte[] out;
        var baos = new ByteArrayOutputStream(50);
        try (var output = new Output(baos)) {
            kryo.writeObject(output, packet);

            output.flush();
        }
        out = baos.toByteArray();

        var restored = kryo.readObject(new Input(out), Packet.class);

        assertEquals(timestamp, restored.timestamp());
        assertNull(restored.payload());
        assertEquals(wantedKey, new String(restored.key()));
    }

    @Test
    void testSerdeNoTimestamp() throws IOException {
        String wantedKey = "my-key" + System.currentTimeMillis();

        Packet packet = new Packet();
        packet.key(wantedKey.getBytes(StandardCharsets.UTF_8));

        byte[] out;
        var baos = new ByteArrayOutputStream(50);
        try (var output = new Output(baos)) {
            kryo.writeObject(output, packet);

            output.flush();
        }
        out = baos.toByteArray();

        var restored = kryo.readObject(new Input(out), Packet.class);

        assertEquals(0, restored.timestamp());
        assertNull(restored.payload());
        assertEquals(wantedKey, new String(restored.key()));
    }

}