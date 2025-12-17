package s7i.flink.meshcore;

import static io.github.s7i.meshtastic.intelligence.TextMessageNotification.TEXT_HTTP_URI;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import s7i.flink.Configuration;
import s7i.flink.Gotify;
import s7i.flink.JobStub;

@Slf4j
public class TextMessageJob extends JobStub {

    public TextMessageJob(ParameterTool params, StreamExecutionEnvironment env,
          Configuration cfg) {
        super(params, env, cfg);
    }

    public static class Sink extends RichSinkFunction<TextMessage> {

        private transient Gotify gotify;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);

            var uri = getRuntimeContext()
                  .getExecutionConfig()
                  .getGlobalJobParameters()
                  .toMap()
                  .get(TEXT_HTTP_URI);

            gotify = new Gotify(uri);
        }

        @Override
        public void invoke(TextMessage value, Context context) throws Exception {
            gotify.notify(value.getFrom(), value.getMessage());
        }
    }

    @Override
    public void build() {
        fromKafkaSource()
              .process(new ProcessDecoded())
              .uid("mc-text-proc")
              .addSink(new Sink())
              .uid("pc-text-sink");

        try {
            env.execute(jobName());
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
