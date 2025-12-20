package s7i.flink.meshcore;

import static io.github.s7i.meshtastic.intelligence.TextMessageNotification.TEXT_HTTP_URI;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
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
            gotify.notify(value.getFrom(), value.getMessage());
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


    public static class WellKnowWindow extends ProcessAllWindowFunction<TextMessage, TextMessage, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<TextMessage, TextMessage, TimeWindow>.Context context,
                            Iterable<TextMessage> elements,
                            Collector<TextMessage> out) throws Exception {

            var msgBuilder = new StringBuilder();
            var counter = new AtomicInteger();

            elements.forEach(textMessage -> {
                int msgNo = counter.incrementAndGet();
                if (msgNo > 1) {
                    msgBuilder.append("\n").append("---").append("\n");
                }
                msgBuilder.append(textMessage.getFrom())
                        .append("\n");
                msgBuilder.append(textMessage.getMessage()).append("\n");
            });

            out.collect(new TextMessage(
                    "Messages", msgBuilder.toString()
            ));

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
        wellKnow.windowAll(TumblingEventTimeWindows.of(Time.of(wellKnowWindow, TimeUnit.MINUTES)))
                .process(new WellKnowWindow())
                .uid("well-known-window")
                .name("well-known-window")
                .addSink(new Sink());

        return all;
    }
}
