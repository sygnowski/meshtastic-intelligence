package io.github.s7i.meshtastic.intelligence.io;

import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class PacketDeserializer implements KafkaRecordDeserializationSchema<Packet> {
    @Override
    public TypeInformation<Packet> getProducedType() {
        return TypeInformation.of(Packet.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Packet> collector) throws IOException {
        var pck = new Packet();

        pck.timestamp(consumerRecord.timestamp());

        var key = consumerRecord.key();
        pck.key(key);

        var value = consumerRecord.value();
        pck.payload(value);

        collector.collect(pck);
    }
}
