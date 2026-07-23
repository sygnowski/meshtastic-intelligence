package s7i.flink.meshcore;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import s7i.flink.Gotify;
import s7i.flink.Utils;
import s7i.flink.meshcore.model.TextMessage;

import static io.github.s7i.meshtastic.intelligence.TextMessageNotification.TEXT_HTTP_URI;

public class GotifySink extends RichSinkFunction<TextMessage> implements Utils.WithJobGlobalConfig {

    private transient Gotify gotify;
    private transient IntCounter httpCallCount;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        var map = jobConfigMap();
        var uri = map.get(TEXT_HTTP_URI);

        gotify = new Gotify(uri);

        httpCallCount = getRuntimeContext().getIntCounter("gotify-http-call-count");
    }

    @Override
    public void invoke(TextMessage value, Context context) throws Exception {
        httpCallCount.add(1);
        gotify.notify(value.getFrom(), value.getText());
    }
}
