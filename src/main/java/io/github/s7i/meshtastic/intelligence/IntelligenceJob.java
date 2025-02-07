package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import java.io.IOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class IntelligenceJob {


    public static class MeshRowDeserializer implements DeserializationSchema<Packet> {

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


    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerTypeWithKryoSerializer(Packet.class, PacketSerializer.class);

            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            KafkaSource<Packet> kafka = KafkaSource.<Packet>builder()
                  .setProperties(new Properties())
                  .setTopics("meshtastic-from-radio")
                  .setValueOnlyDeserializer(new MeshRowDeserializer())
                  .build();

            var stream = env.fromSource(kafka,
                  WatermarkStrategy.forMonotonousTimestamps(), "mesh packets")
                  .filter(packet -> FromRadio.parseFrom(packet.payload)
                        .getPayloadVariantCase() == PayloadVariantCase.PACKET)
                  .map( packet -> {
                      var fromRadio = FromRadio.parseFrom(packet.payload);
                      MeshPacket pkt = fromRadio.getPacket();

                      int pktTo = pkt.getTo();
                      int pktFrom = pkt.getFrom();

                      log.info("reading {}", fromRadio);
                        return Row.of(pktTo, pktFrom);
                    });

            var fromToTable = tableEnv.fromDataStream(stream).as("from", "to");

//            DataStream<Row> dataStream = env.fromElements(
//                  Row.of("Alice", 12),
//                  Row.of("Bob", 10),
//                  Row.of("Alice", 100));
//
//
//            Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
//
//            tableEnv.createTemporaryView("InputTable", inputTable);
//            Table resultTable = tableEnv.sqlQuery(
//                  "SELECT name, SUM(score) FROM InputTable GROUP BY name");
//
//            DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
//
//            resultStream.print();
            env.execute();
        } catch (Exception e) {
            log.error("oops", e);
        }
    }
}
