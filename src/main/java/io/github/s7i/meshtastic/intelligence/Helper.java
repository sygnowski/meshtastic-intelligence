package io.github.s7i.meshtastic.intelligence;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public interface Helper {

    enum Singleton implements Helper {
        INSTANCE;
    }


    default LocalDateTime fromEpoch(long timestamp) {
        return Instant.ofEpochSecond(timestamp)
              .atZone(ZoneOffset.systemDefault())
              .toLocalDateTime();
    }

    default long asUnsigned(int nodeId) {
        return nodeId & 0xffffffffL;
    }

}
