package io.github.s7i.meshtastic.intelligence.nodeinfo;

import com.geeksville.mesh.MeshProtos.NodeInfo;
import com.geeksville.mesh.MeshProtos.User;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.flink.types.Row;

public interface NodeInfoToRowMapper {

    default Row mapNodeInfo(NodeInfo nodeInfo) {
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
    }

}
