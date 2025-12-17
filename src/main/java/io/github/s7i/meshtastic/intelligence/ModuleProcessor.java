package io.github.s7i.meshtastic.intelligence;

import s7i.flink.io.Packet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ModuleProcessor extends KeyedProcessFunction<String, Packet, Packet> {

    private final Set<String> allowed = new HashSet<>();

    public ModuleProcessor(List<String> toAllow) {
        allowed.addAll(toAllow);
    }


    @Override
    public void processElement(Packet value, KeyedProcessFunction<String, Packet, Packet>.Context ctx, Collector<Packet> out) throws Exception {

        if (allowed.contains(TextMessageJob.ANY_MODULE) || allowed.contains(ctx.getCurrentKey())) {
            out.collect(value);
        }
    }
}
