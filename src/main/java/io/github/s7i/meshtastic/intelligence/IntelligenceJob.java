package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import io.github.s7i.meshtastic.intelligence.io.MeshRowDeserializer;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import io.github.s7i.meshtastic.intelligence.io.PacketSerializer;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class IntelligenceJob {


    public static void main(String[] args) {
        try {
            var params = ParameterTool.fromArgs(args);
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerTypeWithKryoSerializer(Packet.class, PacketSerializer.class);

            var cfg = Configuration.from(params.get("cfg"));
            env.getConfig().setGlobalJobParameters(params);

            var tableEnv = StreamTableEnvironment.create(env);
            var kafka = KafkaSource.<Packet>builder()
                  .setProperties(new Properties())
                  .setTopics(cfg.getTopic("source"))
                  .setValueOnlyDeserializer(new MeshRowDeserializer())
                  .build();

            var stream = env.fromSource(kafka,
                  WatermarkStrategy.forMonotonousTimestamps(), "mesh packets")
                  .filter(packet -> FromRadio.parseFrom(packet.payload())
                        .getPayloadVariantCase() == PayloadVariantCase.PACKET)
                  .map( packet -> {
                      var fromRadio = FromRadio.parseFrom(packet.payload());
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
            env.execute(cfg.getName());
        } catch (Exception e) {
            log.error("oops", e);
        }
    }
}
