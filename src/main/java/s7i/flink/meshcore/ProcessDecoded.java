package s7i.flink.meshcore;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import s7i.flink.Utils;
import s7i.flink.meshcore.model.ChannelKey;
import s7i.flink.meshcore.model.MeshcoreDecoded;
import s7i.flink.meshcore.model.TextMessage;

@Slf4j
public class ProcessDecoded extends KeyedProcessFunction<ChannelKey, MeshcoreDecoded, TextMessage> implements TextMessageMapper {

    public static final OutputTag<TextMessage> WELL_KNOWN = new OutputTag<>(
          "well_known",
          TypeInformation.of(TextMessage.class)
    );
    private transient List<String> wellKnowList;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        wellKnowList = Utils.splitAsList(getRuntimeContext()
              .getExecutionConfig()
              .getGlobalJobParameters()
              .toMap()
              .get("well-know-channels"));
    }

    @Override
    public void processElement(MeshcoreDecoded value, KeyedProcessFunction<ChannelKey, MeshcoreDecoded, TextMessage>.Context ctx, Collector<TextMessage> out)
          throws Exception {

        map(value).ifPresent(textMessage -> {
            if (wellKnowList.contains(ctx.getCurrentKey().getHash())) {
                out.collect(textMessage);
            } else {
                ctx.output(WELL_KNOWN, textMessage);
            }
        });
    }
}
