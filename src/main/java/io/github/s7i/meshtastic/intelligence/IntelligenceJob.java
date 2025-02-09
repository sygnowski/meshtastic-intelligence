package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import io.github.s7i.meshtastic.intelligence.Configuration.Topic;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import io.github.s7i.meshtastic.intelligence.io.PacketDeserializer;
import io.github.s7i.meshtastic.intelligence.io.PacketSerializer;
import java.io.StringReader;
import java.time.Duration;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class IntelligenceJob {

    @RequiredArgsConstructor
    public abstract static class JobStub {
        protected final ParameterTool params;
        protected final StreamExecutionEnvironment env;
        protected final Configuration cfg;
        protected StreamTableEnvironment tableEnv;
    }

    public static class MeshJob extends JobStub {

        int watermarkDuration = 5000;
        final Topic source;

        public MeshJob(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg) {
            super(params, env, cfg);
            tableEnv = StreamTableEnvironment.create(env);

            source = cfg.getTopic("source");
        }


        @SneakyThrows
        Properties kafkaProperties() {
            var props = new Properties();
            props.load(new StringReader(source.getKafka()));
            return props;
        }

        void initCatalog() {
            var catalogName = cfg.getOption("catalog.name");
            var databaseName = cfg.getOption("database.name");
            var baseUrl = cfg.getOption("catalog.jdbc");
            var username = cfg.getOption("user.name");
            var password = cfg.getOption("user.password");

            //var loader = IntelligenceJob.class.getClassLoader();
            var catalog = new JdbcCatalog(catalogName, databaseName, username, password, baseUrl);

            tableEnv.registerCatalog(catalogName, catalog);
            tableEnv.useCatalog(catalogName);
            tableEnv.useDatabase(databaseName);

        }

        public SingleOutputStreamOperator<Row> build() {

            initCatalog();

            var kafka = KafkaSource.<Packet>builder()
                  .setProperties(kafkaProperties())
                  .setTopics(source.getName())
                  .setDeserializer(new PacketDeserializer())
                  .build();

            var stream = env.fromSource(kafka,
                        WatermarkStrategy.
                              <Packet>forBoundedOutOfOrderness(Duration.ofMillis(watermarkDuration))
                              .withTimestampAssigner((element, recordTimestamp) -> element.timestamp()), "mesh packets")
                  .name("from kafka: " + source.getName())
                  .filter(packet -> FromRadio.parseFrom(packet.payload())
                        .getPayloadVariantCase() == PayloadVariantCase.PACKET)
                  .map( packet -> {
                      var fromRadio = FromRadio.parseFrom(packet.payload());
                      MeshPacket pkt = fromRadio.getPacket();

                      int pktTo = pkt.getTo();
                      int pktFrom = pkt.getFrom();

                      log.info("reading {}", fromRadio);
                      return Row.of(pktTo, pktFrom);
                  }).returns(Types.ROW_NAMED(
                        new String[] {"from_node", "to_node"},
                        Types.INT, Types.INT
                  ));

            log.debug("catalogs: {}", (Object) tableEnv.listCatalogs());
            tableEnv.executeSql(cfg.getOption("sql.create.mesh.table"));

            var meshNodes = tableEnv.fromDataStream(stream, Schema.newBuilder()
                  .column("from_node", DataTypes.INT())
                  .column("to_node", DataTypes.INT())
                  .build());

            meshNodes.executeInsert("mesh_node_package");

            return stream;
        }
    }

    public static void main(String[] args) {
        try {
            var params = ParameterTool.fromArgs(args);
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerTypeWithKryoSerializer(Packet.class, PacketSerializer.class);

            var cfg = Configuration.from(params.get("mesh-cfg", "/app/mesh.yml"));
            env.getConfig().setGlobalJobParameters(params);

            var stream = new MeshJob(params, env, cfg).build();
            stream.print();

            env.execute(cfg.getName());
        } catch (Exception e) {
            log.error("oops", e);
        }
    }
}
