package io.github.s7i.meshtastic.intelligence;

import io.github.s7i.meshtastic.intelligence.io.Packet;
import io.github.s7i.meshtastic.intelligence.io.PacketDeserializer;
import java.io.BufferedReader;
import java.io.StringReader;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.protocol.types.Field.Str;

@RequiredArgsConstructor
public abstract class JobStub {

    public static final String UUID_KAFKA_SOURCE = "kafka-source";
    int watermarkDuration = 5000;

    protected final ParameterTool params;
    protected final StreamExecutionEnvironment env;
    protected final Configuration cfg;

    public abstract void build();

    public String jobName() {
        var sb = new StringBuilder(cfg.getName());
        cfg.findOption("job.kind").ifPresent(kind -> {
            sb.append(" [kind:").append(kind).append("]");
        });
        sb.append(" | ").append(new GitProps());
        return sb.toString();
    }

    public SingleOutputStreamOperator<Packet> fromKafkaSource() {
        var source = cfg.getTopic("source");

        return env.fromSource(buildSource(source),
                    WatermarkStrategy.
                          <Packet>forBoundedOutOfOrderness(Duration.ofMillis(watermarkDuration))
                          .withTimestampAssigner((element, recordTimestamp) -> element.timestamp()), "mesh packets")
              .uid(UUID_KAFKA_SOURCE)
              .name("from kafka: " + source.getName());
    }

    private KafkaSource<Packet> buildSource(Configuration.Topic source) {
        var kafka = KafkaSource.<Packet>builder()
              .setProperties(kafkaProperties(source))
              .setTopics(source.getName())
              .setDeserializer(new PacketDeserializer());

        cfg.findOption("source.from_timestamp").ifPresent(value -> {
            kafka.setStartingOffsets(OffsetsInitializer.timestamp(parseTimestamp(value)));
        });

        cfg.findOption("source.to_timestamp").ifPresent(value -> {
            kafka.setBounded(OffsetsInitializer.timestamp(parseTimestamp(value)));
        });

        return kafka.build();
    }

    private static long parseTimestamp(String value) {
        return OffsetDateTime.parse(value)
              .toInstant()
              .toEpochMilli();
    }

    @SneakyThrows
    private Properties kafkaProperties(Configuration.Topic source) {
        var props = new Properties();
        try (var br = new BufferedReader(new StringReader(source.getKafka()))) {
            props.load(br);
        }
        return props;
    }
}
