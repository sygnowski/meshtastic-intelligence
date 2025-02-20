package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class MeshJob extends JobStub {
    final StreamTableEnvironment tableEnv;

    public MeshJob(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg) {
        super(params, env, cfg);

        tableEnv = StreamTableEnvironment.create(env);
    }

    public void build() {
        int twSize = Integer.parseInt(cfg.getOption("window.size", "10"));

        var stream = fromKafkaSource()
              .disableChaining()
              .filter(packet -> FromRadio.parseFrom(packet.payload())
                    .getPayloadVariantCase() == PayloadVariantCase.PACKET)
              .name("Packet Payload Filter")
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
              .process(new ProcessNodeWindow())
              .name("Time window (" + twSize + " min.)")
              .addSink(JdbcSink.sink(
                    cfg.getOption("sink.jdbc.sql"),
                    (stmt, row) -> {
                        stmt.setLong(1, row.getFieldAs(0));
                        stmt.setLong(2, row.getFieldAs(1));
                        stmt.setInt(3, row.getFieldAs(2));
                        stmt.setTimestamp(4, new Timestamp(row.getFieldAs(3)));
                        stmt.setTimestamp(5, new Timestamp(row.getFieldAs(4)));
                    },
                    JdbcExecutionOptions.builder()
                          .withBatchSize(100)
                          .withBatchIntervalMs(200)
                          .withMaxRetries(5)
                          .build(),
                    new JdbcConnectionOptionsBuilder()
                          .withUrl(cfg.getOption("sink.jdbc.url"))
                          .withDriverName(cfg.getOption("sink.jdbc.driver"))
                          .withUsername(cfg.getOption("sink.jdbc.user.name"))
                          .withPassword(cfg.getOption("sink.jdbc.user.password"))
                          .build()
              ))
              .name("postgres")
              .disableChaining();
    }


}
