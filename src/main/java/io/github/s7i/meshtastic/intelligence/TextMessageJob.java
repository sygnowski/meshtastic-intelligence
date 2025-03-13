package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.Portnums.PortNum;
import io.github.s7i.meshtastic.intelligence.Model.TextMessage;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class TextMessageJob extends JobStub {

    public TextMessageJob(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg) {
        super(params, env, cfg);
    }

    @Override
    public void build() {

        fromKafkaSource()
              .filter(p -> {
                  var fromRadio = FromRadio.parseFrom(p.payload());
                  return fromRadio.getPayloadVariantCase() == PayloadVariantCase.PACKET &&
                        fromRadio.getPacket().getDecoded().getPortnum() == PortNum.TEXT_MESSAGE_APP;
              })
              .keyBy(p -> FromRadio.parseFrom(p.payload()).getPacket().getFrom() & 0xffffffffL)
              .process(new TextAppProcessor())
              .uid("text-app")
              .name("Text App Processor")
              .addSink(JdbcSink.sink(
                    cfg.getOption("text.sink.jdbc.sql"),
                    (stmt, row) -> {
                        LocalDateTime ldt = row.getFieldAs(TextMessage.TIME);
                        var ts = new Timestamp(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());

                        stmt.setLong(1, row.getFieldAs(TextMessage.FROM_NODE));
                        stmt.setLong(2, row.getFieldAs(TextMessage.TO_NODE));
                        stmt.setInt(3, row.getFieldAs(TextMessage.CHANNEL));
                        stmt.setTimestamp(4, ts);
                        stmt.setString(5, row.getFieldAs(TextMessage.TEXT));
                        stmt.setString(6, row.getFieldAs(TextMessage.JOB_ID));
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
