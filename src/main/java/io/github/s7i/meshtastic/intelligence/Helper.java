package io.github.s7i.meshtastic.intelligence;

import s7i.flink.Utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public interface Helper {

    enum Singleton implements Helper {
        INSTANCE;
    }


    default LocalDateTime fromEpoch(long timestamp) {
        return Utils.fromEpoch(timestamp);
    }

    default long asUnsigned(int nodeId) {
        return nodeId & 0xffffffffL;
    }

}
