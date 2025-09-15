package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import io.github.s7i.meshtastic.intelligence.Model.NodeInfoDescriptor;
import io.github.s7i.meshtastic.intelligence.Model.Notification;
import io.github.s7i.meshtastic.intelligence.Model.SendMessage;
import io.github.s7i.meshtastic.intelligence.Model.TextMessage;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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
          TypeInformation.of(Row.class));


    private transient String messageCharset;
    private transient int accuSize;

    private ListAccumulator<String> accuMessages = new ListAccumulator<>();

    public static final OutputTag<Notification> NOTIFICATION = new OutputTag<>("MTX_NT", TypeInformation.of(Notification.class));

    public static final OutputTag<SendMessage> SEND_MESSAGE_OUTPUT_TAQ = new OutputTag<>("SEND_MESSAGE", TypeInformation.of(SendMessage.class));

    static class NodeInfoStub {

        String name;
        int hopsAway;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Map<String, String> cfg = getRuntimeContext()
              .getExecutionConfig()
              .getGlobalJobParameters()
              .toMap();
        messageCharset = cfg
              .getOrDefault("message.charset", "utf-8");

        accuSize = Integer.parseInt(cfg.getOrDefault("message.accu.size", "10"));

        log.info("Using message charset: {}", messageCharset);

        getRuntimeContext().addAccumulator("messages", accuMessages);
    }

    @Override
    public void processElement(Packet value, KeyedBroadcastProcessFunction<Long, Packet, Row, Row>.ReadOnlyContext ctx, Collector<Row> out) throws Exception {
        final long fromNode = ctx.getCurrentKey();
        final var stub = nodeInfo(ctx);
        MeshPacket packet = FromRadio.parseFrom(value.payload()).getPacket();
        var dec = packet.getDecoded();

        try {
            var plainText = new String(dec.getPayload().toByteArray(), messageCharset);
            var channel = packet.getChannel();

            var textMessage = TextMessage.builder()
                  .text(plainText)
                  .channel(channel)
                  .time(fromEpoch(packet.getRxTime()))
                  .fromNode(fromNode)
                  .toNode(asUnsigned(packet.getTo()))
                  .jobId(getRuntimeContext().getJobId().toHexString())
                  .build();

            if (plainText.trim().equalsIgnoreCase("ping")) {

                var pong = "pong" + hopsAway(stub);

                var send = new SendMessage();
                send.setChannel(channel);
                send.setToNode(packet.getFrom());

                send.setMesssage(pong);

                ctx.output(SEND_MESSAGE_OUTPUT_TAQ, send);
            }

            out.collect(textMessage.toRow());

            String ntMessage = textMessage.getText();

            ntMessage += "\n" + hops(packet) + hopsAway(stub);

            var nt = new Notification()
                  .withTitle(fromNode, channel)
                  .withMessage(textMessage.getTime(), ntMessage);

            stub.ifPresent(ni -> {
                nt.withTitle(ni.name, channel);
            });

            ctx.output(NOTIFICATION, nt);

            accuMessages.add(nt.getTitle() + " | " + nt.getMessage());

            var accuLo = accuMessages.getLocalValue();
            if (accuLo.size() > accuSize) {
                accuLo.remove(0);
            }

        } catch (Exception e) {
            log.warn("can't read message from: {}, why: ", packet, e);
        }
    }

    private static String hopsAway(Optional<NodeInfoStub> stub) {
        return stub.map(s -> ", hops away: " + s.hopsAway)
              .orElse("");
    }

    private static String hops(MeshPacket packet) {
        return "Hops (L: " + packet.getHopLimit() + " S: " + packet.getHopStart() + ")";
    }

    private Optional<NodeInfoStub> nodeInfo(KeyedBroadcastProcessFunction<Long, Packet, Row, Row>.ReadOnlyContext ctx) throws Exception {
        var bni = ctx.getBroadcastState(NODES_BROADCAST).get(ctx.getCurrentKey());
        if (bni != null) {
            var name = bni.getField(NodeInfoDescriptor.USER_NAME.ordinal());
            var longName = bni.getField(NodeInfoDescriptor.USER_LONG_NAME.ordinal());

            var stub = new NodeInfoStub();
            stub.name = name + "/" + longName;
            stub.hopsAway = bni.getFieldAs(NodeInfoDescriptor.HOPS_AWAY.ordinal());

            return Optional.of(stub);
        }
        return Optional.empty();
    }


    @Override
    public void processBroadcastElement(Row value, KeyedBroadcastProcessFunction<Long, Packet, Row, Row>.Context ctx, Collector<Row> out) throws Exception {
        ctx.getBroadcastState(NODES_BROADCAST).put(value.getFieldAs(NodeInfoDescriptor.NODE_ID.ordinal()), value);
    }
}
