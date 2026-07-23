package s7i.flink.meshcore;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import s7i.flink.Configuration;
import s7i.flink.JobStub;
import s7i.flink.io.Packet;
import s7i.flink.meshcore.model.ChannelKey;
import s7i.flink.meshcore.model.MeshcoreDecoded;
import s7i.flink.meshcore.model.TextMessage;

import java.util.concurrent.TimeUnit;

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


    @Override
    public void build() {
        sideOutputWellKnown(
                fromKafkaSource()
                        .map(new Mapper())
                        .keyBy(ChannelKey::from)
                        .process(new ProcessDecoded())
                        .uid("mc-text-proc")
                        .name("mc-text-proc"))
                .addSink(new GotifySink())
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
                .addSink(new GotifySink())
                .uid("sink-well-known")
                .name("sink-well-known");

        return all;
    }
}
