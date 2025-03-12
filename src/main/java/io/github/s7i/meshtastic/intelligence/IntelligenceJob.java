package io.github.s7i.meshtastic.intelligence;

import io.github.s7i.meshtastic.intelligence.Configuration.Option;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import io.github.s7i.meshtastic.intelligence.io.PacketSerializer;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.FlinkRuntimeException;

@Slf4j
public class IntelligenceJob {

    interface JobCreator {

        JobStub create(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg);
    }

    public static void main(String[] args) {
        try {
            var params = ParameterTool.fromArgs(args);

            log.info("command line params: {}", params.toMap());

            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerTypeWithKryoSerializer(Packet.class, PacketSerializer.class);

            var cfg = Configuration.from(params.get("mesh-cfg", "/app/mesh.yml"));
            var map = cfg.getOptions().stream()
                  .filter(option -> option.getName() != null && option.getValue() != null)
                  .collect(Collectors.toMap(Option::getName, Option::getValue));
            map.putAll(params.toMap());
            env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));

            var jobKind = cfg.getOption("job.kind", "default");
            Stream.of(jobKind)
                  .map(kind -> Optional.ofNullable(Map.<String, JobCreator>of(
                        "default", MeshJob::new,
                        "node-info", MeshNodeInfoJob::new,
                        "text-app", TextMessageJob::new
                  ).get(kind)))
                  .flatMap(Optional::stream)
                  .map(jobCreator -> jobCreator.create(params, env, cfg))
                  .findFirst()
                  .orElseThrow(() -> new RuntimeException("unknown job kind" + jobKind))
                  .build();
        } catch (Exception e) {
            log.error("failed to start job", e);
            throw new FlinkRuntimeException(e.getMessage());
        }
    }
}
