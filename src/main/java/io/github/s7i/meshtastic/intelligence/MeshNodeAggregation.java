package io.github.s7i.meshtastic.intelligence;

import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;

@Slf4j
public class MeshNodeAggregation implements AggregateFunction<Row, Accumulator, Row> {
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(Row value, Accumulator accumulator) {
        accumulator.incCount(value);
        return accumulator;
    }

    @Override
    public Row getResult(Accumulator accumulator) {
        return accumulator.getNodeCount()
              .entrySet()
              .stream()
              .findFirst()
              .map(e ->
                    Row.of(
                          e.getKey().getFromNode(),
                          e.getKey().getToNode(),
                          e.getValue())
              )
              .orElseThrow();
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        var nodeCount = new HashMap<>(a.getNodeCount());
        for (var e : b.getNodeCount().entrySet()) {
            nodeCount.computeIfPresent(e.getKey(), ((key, count) -> e.getValue() + count));
        }
        var merge = new Accumulator();
        merge.setNodeCount(nodeCount);
        return merge;
    }
}
