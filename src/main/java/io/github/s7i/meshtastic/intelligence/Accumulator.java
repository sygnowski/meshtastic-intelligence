package io.github.s7i.meshtastic.intelligence;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.apache.flink.types.Row;

@Data
public class Accumulator implements Serializable {

    @Data
    public static class Key implements Serializable {

        public static Key from(Row row) {
            var key = new Key();

            key.setFromNode(row.getFieldAs(0));
            key.setToNode(row.getFieldAs(1));

            return key;
        }

        long fromNode;
        long toNode;
    }

    public Accumulator() {
    }

    public void incCount(Row row) {
        if (nodeCount == null) {
            nodeCount = new HashMap<>();
        }
        requireNonNull(row, "row");

        var key = Key.from(row);
        var count = nodeCount.putIfAbsent(key, 1);
        if (count == null) {
            return;
        }
        count += 1;
        nodeCount.put(key, count);
    }

    private Map<Key, Integer> nodeCount;
}
