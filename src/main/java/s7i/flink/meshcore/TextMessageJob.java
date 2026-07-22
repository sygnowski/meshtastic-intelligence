package s7i.flink.meshcore;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import s7i.flink.Configuration;
import s7i.flink.Gotify;
import s7i.flink.JobStub;
import s7i.flink.Utils;
import s7i.flink.io.Packet;
import s7i.flink.meshcore.model.ChannelKey;
import s7i.flink.meshcore.model.MeshcoreDecoded;
import s7i.flink.meshcore.model.TextMessage;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.s7i.meshtastic.intelligence.TextMessageNotification.TEXT_HTTP_URI;

@Slf4j
public class TextMessageJob extends JobStub {

    private final long wellKnowWindow;

    public TextMessageJob(ParameterTool params, StreamExecutionEnvironment env,
                          Configuration cfg) {
        super(params, env, cfg);


        wellKnowWindow = Long.parseLong(cfg.getOption(
                "well-know-window",
                "60"));
    }

    public static class Sink extends RichSinkFunction<TextMessage> implements Utils.WithJobGlobalConfig {

        private transient Gotify gotify;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);

            var map = jobConfigMap();
            var uri = map.get(TEXT_HTTP_URI);

            gotify = new Gotify(uri);
        }

        @Override
        public void invoke(TextMessage value, Context context) throws Exception {
            gotify.notify(value.getFrom(), value.getText());
        }
    }

    public static class Mapper extends RichMapFunction<Packet, MeshcoreDecoded> {

        private transient MeshcoreDecodedMapper mapper;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);

            mapper = new MeshcoreDecodedMapper();
        }

        @Override
        public MeshcoreDecoded map(Packet packet) throws Exception {
            return mapper.mapDecoded(packet.payload());
        }
    }


    public static class WellKnowWindow extends ProcessWindowFunction<TextMessage, TextMessage, String, TimeWindow> {

        @Override
        public void process(String channelHash, ProcessWindowFunction<TextMessage, TextMessage, String, TimeWindow>.Context context, Iterable<TextMessage> iterable, Collector<TextMessage> out) throws Exception {
            var msgBuilder = new StringBuilder();
            var counter = new AtomicInteger();

            var map = new LinkedHashMap<String, TextMessage>();
            for (var tm : iterable) {
                var preset = map.computeIfPresent(tm.getMessageHash(), (k, v) -> {
                    v.addPath(tm.getPathList().stream().findFirst().orElseThrow());
                    return v;
                });
                if (preset == null) {
                    map.put(tm.getMessageHash(), tm);
                }
            }

            for (String key : map.keySet()) {
                var textMessage = map.get(key);

                int msgNo = counter.incrementAndGet();
                if (msgNo > 1) {
                    msgBuilder.append("\n").append("---").append("\n");
                }
                msgBuilder.append(textMessage.title())
                        .append("\n");
                msgBuilder.append(textMessage.message());
            }

            out.collect(TextMessage.builder()
                    .from("Messages on channel: " + channelHash + " (" + counter.intValue() + ")")
                    .timestamp(Instant.now().getEpochSecond())
                    .text(msgBuilder.toString())
                    .channelHash(channelHash)
                    .build());
        }
    }

    @Override
    public void build() {
        sideOutputWellKnown(
                fromKafkaSource()
                        .map(new Mapper())
                        .keyBy(ChannelKey::from)
                        .process(new ProcessDecoded())
                        .uid("mc-text-proc")
                        .name("mc-text-proc"))
                .addSink(new Sink())
                .uid("mc-text-sink")
                .name("mc-text-sink");

        try {
            env.execute(jobName());
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    SingleOutputStreamOperator<TextMessage> sideOutputWellKnown(SingleOutputStreamOperator<TextMessage> all) {
        var wellKnow = all.getSideOutput(ProcessDecoded.WELL_KNOWN);
        wellKnow.keyBy(TextMessage::getChannelHash)
                .window(TumblingEventTimeWindows.of(Time.of(wellKnowWindow, TimeUnit.MINUTES)))
                .process(new WellKnowWindow())
                .uid("well-known-window")
                .name("well-known-window")
                .addSink(new Sink())
                .uid("sink-well-known")
                .name("sink-well-known");

        return all;
    }
}
