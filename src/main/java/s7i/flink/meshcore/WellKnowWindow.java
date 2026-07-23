package s7i.flink.meshcore;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import s7i.flink.meshcore.model.TextMessage;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class WellKnowWindow extends ProcessWindowFunction<TextMessage, TextMessage, String, TimeWindow> {

    private transient IntCounter mergedCount;
    private transient IntCounter totalCollected;

    @Override
    public void open(Configuration parameters) throws Exception {
        mergedCount = getRuntimeContext().getIntCounter("same-hash-merged-count");
        totalCollected = getRuntimeContext().getIntCounter("total-message-collected");
    }

    @Override
    public void process(String channelHash, ProcessWindowFunction<TextMessage, TextMessage, String, TimeWindow>.Context context, Iterable<TextMessage> iterable, Collector<TextMessage> out) throws Exception {
        var msgBuilder = new StringBuilder();
        var counter = new AtomicInteger();

        var map = new LinkedHashMap<String, TextMessage>();
        for (var tm : iterable) {
            var preset = map.computeIfPresent(tm.getMessageHash(), (k, v) -> {
                tm.getPathList().stream().findFirst().ifPresent(v::addPath);
                mergedCount.add(1);
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

        map.clear();
        totalCollected.add(counter.get());
    }
}
