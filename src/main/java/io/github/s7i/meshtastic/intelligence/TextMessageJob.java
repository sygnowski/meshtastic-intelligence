package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import io.github.s7i.meshtastic.intelligence.Model.MeshCommand;
import io.github.s7i.meshtastic.intelligence.Model.SendMessage;
import io.github.s7i.meshtastic.intelligence.Model.TextMessage;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

@Slf4j
public class TextMessageJob extends JobStub {

    public static final String ANY_MODULE = "_";
    public static final String UNNAMED = "unnamed";

    static List<String> allowedModules(Configuration cfg) {
        return Configuration.split(cfg.getOption("module.allowed", ANY_MODULE)).collect(Collectors.toList());
    }

    public TextMessageJob(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg) {
        super(params, env, cfg);
    }

    @Override
    public void build() {
        var textMessagesWindow = Integer.parseInt(cfg.getOption("text.message.window", "60"));

        var allowed = allowedModules(cfg);

        var packetStream = fromKafkaSource()
              .keyBy(packet ->
                    packet.key() != null ? new String(packet.key()) : UNNAMED)
              .process(new ModuleProcessor(allowed))
              .name("module-processor")
              .process(new NodeAndTextMessageProcessor())
              .name("NodeInfo And TextProcessor")
              .disableChaining();

        var brdStream = packetStream.getSideOutput(NodeAndTextMessageProcessor.NODE_INFO_ROW)
              .broadcast(TextAppProcessor.NODES_BROADCAST);

        var textRowsStream = packetStream.keyBy(p -> FromRadio.parseFrom(p.payload()).getPacket().getFrom() & 0xffffffffL)
              .connect(brdStream)
              .process(new TextAppProcessor())
              .uid("text-app")
              .name("Text App Processor");

        var sendCfg = cfg.getTopic("send-message");
        var props = kafkaProperties(sendCfg);

        textRowsStream.getSideOutput(TextAppProcessor.SEND_MESSAGE_OUTPUT_TAQ)
              .sinkTo(KafkaSink.<SendMessage>builder()
                    .setRecordSerializer(new SendMessageKafkaSerializer(
                          sendCfg.getName(),
                          cfg.getOption("send.message.node")
                    ))
                    .setKafkaProducerConfig(props)
                    .build())
              .disableChaining()
              .uid("send-message-sink")
              .name("Send Message Sink");

        var meshCommand = cfg.getTopic("mesh-command");
        textRowsStream.getSideOutput(TextAppProcessor.CMD_OUTPUT_TAQ)
              .sinkTo(KafkaSink.<MeshCommand>builder()
                    .setRecordSerializer(new MeshCommandKafkaSerializer(meshCommand.getName()))
                    .setKafkaProducerConfig(kafkaProperties(meshCommand))
                    .build())
              .disableChaining()
              .uid("mesh-cmd-sink")
              .name("Mesh Command Sink");

        var ntWindowStream = textRowsStream.getSideOutput(TextAppProcessor.NOTIFICATION_WINDOW_OUTPUT_TAQ)
              .windowAll(TumblingEventTimeWindows.of(Time.of(textMessagesWindow, TimeUnit.MINUTES)))
              .process(new TextMessageWindow())
              .name("Windowed Messages")
              .uid("windowed-messages");

        textRowsStream.getSideOutput(TextAppProcessor.NOTIFICATION)
              .union(ntWindowStream)
              .addSink(new TextMessageNotification())
              .disableChaining()
              .name("Http Notifications");

        if (cfg.hasFlag("flags", "no-database")) {
            textRowsStream.addSink(new DiscardingSink<>())
                  .name("discard-sink");
        } else {
            textRowsStream.addSink(JdbcSink.sink(
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
                        getJdbcExecutionOptions(),
                        getJdbcConnectionOptions()
                  ))
                  .name("postgres")
                  .disableChaining();
        }

        try {
            env.executeAsync(jobName());
        } catch (Exception e) {
            log.error("job execution");
        }
    }
}
