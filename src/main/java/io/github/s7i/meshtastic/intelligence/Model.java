package io.github.s7i.meshtastic.intelligence;

import java.time.LocalDateTime;
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

        public static Notification from(Row value) {
            var title = title(
                  value.getField(TextMessage.FROM_NODE),
                  value.getField(TextMessage.CHANNEL)
            );
            var message = message(
                  value.getField(TextMessage.TIME),
                  value.getField(TextMessage.TEXT)
            );
            Notification notification = new Notification();
            notification.setTitle(title);
            notification.setMessage(message);
            return notification;
        }

        public static Notification from(String from, String chann, String time, String text) {

            Notification notification = new Notification();
            notification.setTitle(title(from, chann));
            notification.setMessage(message(time, text));
            return notification;
        }

        static String title(Object from, Object chann) {
            return String.format("Message from node: (%s) on channel: (%s)",
                  from, chann
            );
        }

        static String message(Object time, Object text) {
            return String.format("Time: %s\nText: %s",
                  time, text
            );
        }


        private String title;
        private String message;
    }

    public static class NodeInfo {

        public static final String USER_NAME = "user_name";
        public static final String USER_LONG_NAME = "user_long_name";
        public static final String NODE_ID = "node_id";
        public static final String[] FIELDS = {
              NODE_ID, "user_id", USER_NAME, USER_LONG_NAME, "user_role", "last_heard", "hops_away", "snr"
        };

        public static int pos(String field) {
            for (int i = 0; FIELDS.length > i; i++) {
                if (field.equals(FIELDS[i])) {
                    return i;
                }
            }
            throw new IllegalStateException();
        }
    }

}
