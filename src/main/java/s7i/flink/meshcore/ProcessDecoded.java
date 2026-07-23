package s7i.flink.meshcore;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import s7i.flink.Utils;
import s7i.flink.meshcore.model.ChannelKey;
import s7i.flink.meshcore.model.MeshcoreDecoded;
import s7i.flink.meshcore.model.TextMessage;

@Slf4j
public class ProcessDecoded extends KeyedProcessFunction<ChannelKey, MeshcoreDecoded, TextMessage>
        implements TextMessageMapper, Utils.WithJobGlobalConfig {

    public static final OutputTag<TextMessage> WELL_KNOWN = new OutputTag<>(
            "well_known",
            TypeInformation.of(TextMessage.class)
    );
    private transient List<String> wellKnowList;
    private transient Counter wellKnowCounter;
    private transient Counter specialCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        var configMap = jobConfigMap();
        String cfgWellKnowList = configMap.get("well-known-channels");

        log.info("well known list: {}, config map: {}", cfgWellKnowList, configMap);

        wellKnowList = Utils.splitAsList(cfgWellKnowList);

        if (wellKnowList.isEmpty()) {
            throw new IllegalStateException("empty well known list");
        }

        var meshcore = getRuntimeContext().getMetricGroup().addGroup("meshcore");
        wellKnowCounter = meshcore.counter("well-know");
        specialCounter = meshcore.counter("special");
    }

    @Override
    public void processElement(MeshcoreDecoded value, KeyedProcessFunction<ChannelKey, MeshcoreDecoded, TextMessage>.Context ctx, Collector<TextMessage> out)
            throws Exception {

        map(value).ifPresent(textMessage -> {
            if (wellKnowList.stream().anyMatch(wki -> wki.equals(ctx.getCurrentKey().getHash()))) {
                ctx.output(WELL_KNOWN, textMessage);
                wellKnowCounter.inc();
            } else {
                out.collect(textMessage.asPretty());
                specialCounter.inc();
            }
        });
    }
}
