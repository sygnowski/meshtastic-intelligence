package io.github.s7i.meshtastic.intelligence;

import lombok.Value;
import org.apache.flink.types.Row;

public abstract class Model {

    @Value
    public static class NodeContext {

        public static NodeContext from(Row model) {
            return new NodeContext(
                  model.getFieldAs("from_node"),
                  model.getFieldAs("to_node"),
                  model.getFieldAs("kind")
            );
        }

        long fromNode;
        long toNode;
        String kind;
    }

    public static Row asNodeContext(long formNode, long toNode, String kind) {
        var model = Row.withNames();

        model.setField("from_node", formNode);
        model.setField("to_node", toNode);
        model.setField("kind", kind);

        return model;
    }

}
