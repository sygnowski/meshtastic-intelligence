package io.github.s7i.meshtastic.intelligence;

import io.github.s7i.meshtastic.intelligence.io.Packet;
import io.github.s7i.meshtastic.intelligence.io.PacketSerializer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class IntelligenceJob {

    public static void main(String[] args) {
        try {
            var params = ParameterTool.fromArgs(args);
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerTypeWithKryoSerializer(Packet.class, PacketSerializer.class);

            var cfg = Configuration.from(params.get("mesh-cfg", "/app/mesh.yml"));
            env.getConfig().setGlobalJobParameters(params);

            var jobKind = params.get("job-kind", "default");
            Map.<String, Supplier<JobStub>>of(
                  "default",
                  () -> new MeshJob(params, env, cfg)
            ).entrySet()
                  .stream()
                  .filter( e -> e.getKey().equals(jobKind))
                  .map(Entry::getValue)
                  .map(Supplier::get)
                  .findFirst()
                  .orElseThrow(() -> new RuntimeException("unknown job kind" + jobKind))
                  .build();

            env.execute(cfg.getName());
        } catch (Exception e) {
            log.error("oops", e);
        }
    }
}
