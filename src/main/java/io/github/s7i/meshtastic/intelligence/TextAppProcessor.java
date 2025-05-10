package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.MeshPacket;
import io.github.s7i.meshtastic.intelligence.Model.NodeInfo;
import io.github.s7i.meshtastic.intelligence.Model.Notification;
import io.github.s7i.meshtastic.intelligence.Model.SendMessage;
import io.github.s7i.meshtastic.intelligence.Model.TextMessage;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import java.util.Map;
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

    public static final OutputTag<Notification> NOTIFICATION = new OutputTag<Notification>("MTX_NT") {

    };

    public static final OutputTag<SendMessage> SEND_MESSAGE_OUTPUT_TAQ = new OutputTag<>("SEND_MESSAGE") {

    };

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        Map<String, String> cfg = getRuntimeContext()
              .getExecutionConfig()
              .getGlobalJobParameters()
              .toMap();
        messageCharset = cfg
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

            var textMessage = TextMessage.builder()
                  .text(plainText)
                  .channel(channel)
                  .time(fromEpoch(packet.getRxTime()))
                  .fromNode(ctx.getCurrentKey())
                  .toNode(asUnsigned(packet.getTo()))
                  .jobId(getRuntimeContext().getJobId().toHexString())
                  .build();

            if (plainText.trim().equalsIgnoreCase("ping")) {

                var toNode = (int) (ctx.getCurrentKey() & 0xffffffffL);

                var send = new SendMessage();
                send.setChannel(channel);
                send.setToNode(toNode);
                send.setMesssage("pong");

                ctx.output(SEND_MESSAGE_OUTPUT_TAQ, send);
            }


            var row = textMessage
                  .toRow();

            out.collect(row);

            var bni = ctx.getBroadcastState(NODES_BROADCAST).get(ctx.getCurrentKey());
            Notification nt = Notification.from(row);
            if (bni != null) {
                var name = bni.getField(NodeInfo.pos(NodeInfo.USER_NAME));
                var longName = bni.getField(NodeInfo.pos(NodeInfo.USER_LONG_NAME));

                nt.setTitle(Notification.title(name + "/" + longName, channel));
            }

            ctx.output(NOTIFICATION, nt);

            accuMessages.add(nt.getTitle() + " | " + nt.getMessage());

            var accuLo = accuMessages.getLocalValue();
            if (accuLo.size() > 10) {
                accuLo.remove(0);
            }

        } catch (Exception e) {
            log.warn("can't read message from: {}, why: ", packet, e);
        }
    }


    @Override
    public void processBroadcastElement(Row value, KeyedBroadcastProcessFunction<Long, Packet, Row, Row>.Context ctx, Collector<Row> out) throws Exception {
        ctx.getBroadcastState(NODES_BROADCAST).put(value.getFieldAs(0), value);
    }
}
