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

    public MeshJob(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg) {
        super(params, env, cfg);
    }

    public void build() {
        int twSize = Integer.parseInt(cfg.getOption("window.size", "10"));

        fromKafkaSource()
              .disableChaining()
              .filter(packet -> FromRadio.parseFrom(packet.payload())
                    .getPayloadVariantCase() == PayloadVariantCase.PACKET)
              .name("Packet Payload Filter")
              .disableChaining()
              .map(packet -> {
                  var fromRadio = FromRadio.parseFrom(packet.payload());

                  var payloadKind = "";
                  var mshPacket = fromRadio.getPacket();
                  if (mshPacket.hasEncrypted()) {
                      payloadKind = "ENCRYPTED";

                  } else if (mshPacket.hasDecoded()) {
                      payloadKind = mshPacket.getDecoded().getPortnum().name();
                  }

                  MeshPacket pkt = fromRadio.getPacket();

                  long pktFrom = pkt.getFrom() & 0xffffffffL;
                  long pktTo = pkt.getTo() & 0xffffffffL;

                  return Model.asNodeContext(pktFrom, pktTo, payloadKind);
              }).name("map as Row with Package Kind")
              .keyBy(value -> value)
              .window(TumblingEventTimeWindows.of(Time.of(twSize, TimeUnit.MINUTES)))
              .process(new ProcessNodeWindow())
              .name("Time window (" + twSize + " min.)")
              .addSink(JdbcSink.sink(
                    cfg.getOption("sink.jdbc.sql"),
                    (stmt, row) -> {
                        stmt.setLong(1, row.getFieldAs("from"));
                        stmt.setLong(2, row.getFieldAs("to"));
                        stmt.setInt(3, row.getFieldAs("count"));
                        stmt.setTimestamp(4, new Timestamp(row.getFieldAs("start")));
                        stmt.setTimestamp(5, new Timestamp(row.getFieldAs("end")));
                        stmt.setString(6, row.getFieldAs("kind"));
                        stmt.setString(7, row.getFieldAs("jobId"));
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

        try {
            env.executeAsync(jobName());
        } catch (Exception e) {
            log.error("job execution");
        }
    }
}
