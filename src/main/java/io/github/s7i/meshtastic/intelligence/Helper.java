package io.github.s7i.meshtastic.intelligence;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public interface Helper {


    default LocalDateTime fromEpoch(long timestamp) {
        return Instant.ofEpochSecond(timestamp)
              .atZone(ZoneOffset.systemDefault())
              .toLocalDateTime();
    }

    default long asUnsigned(int nodeId) {
        return nodeId & 0xffffffffL;
    }

}
