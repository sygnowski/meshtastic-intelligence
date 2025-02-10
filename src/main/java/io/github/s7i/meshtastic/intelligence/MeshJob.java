package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import io.github.s7i.meshtastic.intelligence.Configuration.Topic;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import io.github.s7i.meshtastic.intelligence.io.PacketDeserializer;
import java.io.StringReader;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class MeshJob extends JobStub {

    int watermarkDuration = 5000;
    final Topic source;
    final StreamTableEnvironment tableEnv;

    public MeshJob(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg) {
        super(params, env, cfg);
        source = cfg.getTopic("source");

        tableEnv = StreamTableEnvironment.create(env);
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

        log.debug("Catalog :: List databases: {}", catalog.listDatabases());

        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        //tableEnv.useDatabase(databaseName);

        log.debug("catalogs: {}", (Object) tableEnv.listCatalogs());

        cfg.onTrue("user.catalog.sql.create",
              () -> tableEnv.executeSql(cfg.getOption("sql.create.mesh.table")));
    }

    public void build() {

        cfg.onTrue("use.catalog", this::initCatalog);

        int twSize = Integer.parseInt(cfg.getOption("window.size", "10"));

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
              .disableChaining()
              .filter(packet -> FromRadio.parseFrom(packet.payload())
                    .getPayloadVariantCase() == PayloadVariantCase.PACKET)
              .name("Packet Payload")
              .disableChaining()
              .map(packet -> {
                  var fromRadio = FromRadio.parseFrom(packet.payload());
                  MeshPacket pkt = fromRadio.getPacket();

                  long pktFrom = pkt.getFrom() & 0xffffffffL;
                  long pktTo = pkt.getTo() & 0xffffffffL;

                  return Row.of(pktFrom, pktTo);
              }).returns(Types.ROW_NAMED(
                    new String[]{"from_node", "to_node"},
                    Types.LONG, Types.LONG
              ))
              .keyBy(value -> value.getField(0) + "-" + value.getField(1))
              .window(TumblingEventTimeWindows.of(Time.of(twSize, TimeUnit.MINUTES)))
              .aggregate(new MeshNodeAggregation())
              .addSink(JdbcSink.sink(
                    cfg.getOption("sink.jdbc.sql"),
                    (stmt, row) -> {
                        stmt.setLong(1, row.getFieldAs(0));
                        stmt.setLong(2, row.getFieldAs(1));
                        stmt.setInt(3, row.getFieldAs(2));
                    },
                    JdbcExecutionOptions.builder()
                          .withBatchSize(100)
                          .withBatchIntervalMs(200)
                          .withMaxRetries(5)
                          .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                          .withUrl(cfg.getOption("sink.jdbc.url"))
                          .withDriverName(cfg.getOption("sink.jdbc.driver"))
                          .withUsername(cfg.getOption("sink.jdbc.user.name"))
                          .withPassword(cfg.getOption("sink.jdbc.user.password"))
                          .build()
              ))
              .name("postgres")
              .disableChaining();

//        var meshNodes = tableEnv.fromDataStream(stream, Schema.newBuilder()
//              .column("from_node", DataTypes.INT())
//              .column("to_node", DataTypes.INT())
//              .build());
//
//        tableEnv.createTemporaryView("nodes_status", meshNodes);
//
//        stream.print();
    }
}
