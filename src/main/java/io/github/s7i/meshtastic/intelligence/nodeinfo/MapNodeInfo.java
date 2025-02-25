package io.github.s7i.meshtastic.intelligence.nodeinfo;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.NodeInfo;
import com.geeksville.mesh.MeshProtos.User;
import io.github.s7i.meshtastic.intelligence.io.Packet;
import java.time.Instant;
import java.time.ZoneOffset;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

@Slf4j
public class MapNodeInfo extends RichMapFunction<Packet, Row> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public Row map(Packet value) throws Exception {
        var fromRadio = FromRadio.parseFrom(value.payload());
        try {

            NodeInfo nodeInfo = fromRadio.getNodeInfo();
            User user = nodeInfo.getUser();

            var lastHeard = nodeInfo.getLastHeard() != 0
                  ? Instant.ofEpochSecond(nodeInfo.getLastHeard())
                  .atZone(ZoneOffset.systemDefault())
                  .toLocalDateTime()
                  : null;

            long nodeId = nodeInfo.getNum() & 0xffffffffL;
            String userId = user.getId();
            return Row.of(
                  nodeId,
                  userId,
                  user.getShortName(),
                  user.getLongName(),
                  user.getRole().toString(),
                  lastHeard,
                  nodeInfo.getHopsAway(),
                  nodeInfo.getSnr()
            );
        } catch (Exception e) {
            log.error("can't parse: {}", fromRadio);
            throw e;
        }
    }
}
