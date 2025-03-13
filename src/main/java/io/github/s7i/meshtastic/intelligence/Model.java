package io.github.s7i.meshtastic.intelligence;

import java.time.LocalDateTime;
import lombok.Builder;
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


    @Value
    @Builder
    public static class TextMessage {

        public static final String JOB_ID = "job_id";
        public static final String TEXT = "text";
        public static final String CHANNEL = "channel";
        public static final String TIME = "time";
        public static final String TO_NODE = "to_node";
        public static final String FROM_NODE = "from_node";
        long fromNode;
        long toNode;
        LocalDateTime time;
        int channel;
        String text;
        String jobId;

        public Row toRow() {
            var row = Row.withNames();

            row.setField(FROM_NODE, fromNode);
            row.setField(TO_NODE, toNode);
            row.setField(TIME, time);
            row.setField(CHANNEL, channel);
            row.setField(TEXT, text);
            row.setField(JOB_ID, jobId);

            return row;
        }
    }

    public static Row asNodeContext(long formNode, long toNode, String kind) {
        var model = Row.withNames();

        model.setField("from_node", formNode);
        model.setField("to_node", toNode);
        model.setField("kind", kind);

        return model;
    }

}
