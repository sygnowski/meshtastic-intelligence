package io.github.s7i.meshtastic.intelligence.io;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class MeshRowDeserializer implements DeserializationSchema<Packet> {

    @Override
    public Packet deserialize(byte[] message) throws IOException {
        return new Packet(message);
    }

    @Override
    public boolean isEndOfStream(Packet nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Packet> getProducedType() {
        return TypeInformation.of(Packet.class);
    }
}
