package io.github.s7i.meshtastic.intelligence;

import java.time.LocalDateTime;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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
    @Builder(toBuilder = true)
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

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Notification {

        public Notification withTitle(Object from, Object chann) {
            title = String.format("Message from node: (%s) on channel: (%s)",
                  from, chann
            );
            return this;
        }

        public Notification withMessage(Object time, Object text) {
            message = String.format("Time: %s\nText: %s",
                  time, text
            );
            return this;
        }


        private String title;
        private String message;
    }

    public enum NodeInfoDescriptor {
        NODE_ID("node_id"),
        USER_ID("user_id"),
        USER_NAME("user_name"),
        USER_LONG_NAME("user_long_name"),
        USER_ROLE("user_role"),
        LAST_HEARD("last_heard"),
        HOPS_AWAY("hops_away"),
        SNR("snr");


        NodeInfoDescriptor(String name) {
            NAME = name;
        }

        public final String NAME;

        public static final String[] FIELDS = Arrays.stream(values())
              .map(d -> d.NAME)
              .toArray(String[]::new);
    }

    @Data
    public static class SendMessage {

        private int channel;
        private int toNode;
        private String messsage;
    }

}
