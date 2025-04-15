package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import io.github.s7i.meshtastic.intelligence.Model.TextMessage;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Slf4j
public class TextAppProcessor extends KeyedBroadcastProcessFunction<Long, Packet, Row, Row> implements Helper {


    public static final MapStateDescriptor<Long, Row> NODES_BROADCAST = new MapStateDescriptor<>(
          "NodesBroadcast",
          BasicTypeInfo.LONG_TYPE_INFO,
          TypeInformation.of(new TypeHint<Row>() {
          }));


    private transient String messageCharset;

    private ListAccumulator<String> accuMessages = new ListAccumulator<>();

    public static final OutputTag<Row> NOTIFICATION = new OutputTag<Row>("MTX_NT") {

    };

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        messageCharset = getRuntimeContext()
              .getExecutionConfig()
              .getGlobalJobParameters()
              .toMap()
              .getOrDefault("message.charset", "utf-8");

        log.info("Using message charset: {}", messageCharset);

        getRuntimeContext().addAccumulator("messages", accuMessages);
    }

    @Override
    public void processElement(Packet value, KeyedBroadcastProcessFunction<Long, Packet, Row, Row>.ReadOnlyContext ctx, Collector<Row> out) throws Exception {
        MeshPacket packet = FromRadio.parseFrom(value.payload()).getPacket();
        var dec = packet.getDecoded();

        try {
            var plainText = new String(dec.getPayload().toByteArray(), messageCharset);
            var channel = packet.getChannel();

            var row = TextMessage.builder()
                  .text(plainText)
                  .channel(channel)
                  .time(fromEpoch(packet.getRxTime()))
                  .fromNode(ctx.getCurrentKey())
                  .toNode(asUnsigned(packet.getTo()))
                  .jobId(getRuntimeContext().getJobId().toHexString())
                  .build()
                  .toRow();

            out.collect(row);

            ctx.output(NOTIFICATION, row);

            accuMessages.add(String.format("%s form: %d, chan: %s, text: %s",
                  fromEpoch(packet.getRxTime()), ctx.getCurrentKey(), channel, plainText));

        } catch (Exception e) {
            log.warn("can't read message from: {}", packet);
        }
    }


    @Override
    public void processBroadcastElement(Row value, KeyedBroadcastProcessFunction<Long, Packet, Row, Row>.Context ctx, Collector<Row> out) throws Exception {
        ctx.getBroadcastState(NODES_BROADCAST).put(value.getFieldAs(1), value);
    }
}
