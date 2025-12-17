package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.Portnums.PortNum;
import s7i.flink.io.Packet;
import io.github.s7i.meshtastic.intelligence.nodeinfo.NodeInfoToRowMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class NodeAndTextMessageProcessor extends ProcessFunction<Packet, Packet> implements NodeInfoToRowMapper {

    public static final OutputTag<Row> NODE_INFO_ROW = new OutputTag<>("NODE_INFO_ROW") {

    };

    @Override
    public void processElement(Packet value, ProcessFunction<Packet, Packet>.Context ctx, Collector<Packet> out) throws Exception {

        var fromRadio = FromRadio.parseFrom(value.payload());

        if (fromRadio.getPayloadVariantCase() == PayloadVariantCase.PACKET &&
              fromRadio.getPacket().getDecoded().getPortnum() == PortNum.TEXT_MESSAGE_APP) {
            out.collect(value);

        } else if (fromRadio.getPayloadVariantCase() == PayloadVariantCase.NODE_INFO) {
            ctx.output(NODE_INFO_ROW, mapNodeInfo(fromRadio.getNodeInfo()));
        }
    }
}
