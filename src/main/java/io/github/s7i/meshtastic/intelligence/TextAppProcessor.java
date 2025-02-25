package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

@Slf4j
public class TextAppProcessor extends KeyedProcessFunction<Long, Packet, Row> implements Helper {

    @Override
    public void processElement(Packet value, KeyedProcessFunction<Long, Packet, Row>.Context ctx, Collector<Row> out) throws Exception {
        MeshPacket packet = FromRadio.parseFrom(value.payload()).getPacket();
        var dec = packet.getDecoded();

        try {
            var plainText = new String(dec.getPayload().toByteArray());
            var channel = packet.getChannel();

            out.collect(Row.of(
                  fromEpoch(packet.getRxTime()),
                  channel,
                  ctx.getCurrentKey(),
                  asUnsigned(packet.getTo()),
                  plainText
            ));
        } catch (Exception e) {
            log.warn("can't read message from: {}", packet);
        }
    }
}
