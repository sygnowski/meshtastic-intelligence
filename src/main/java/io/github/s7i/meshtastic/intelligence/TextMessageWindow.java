package io.github.s7i.meshtastic.intelligence;

import io.github.s7i.meshtastic.intelligence.Model.Notification;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TextMessageWindow extends ProcessAllWindowFunction<Notification, Notification, TimeWindow> {

    @Override
    public void process(ProcessAllWindowFunction<Notification, Notification, TimeWindow>.Context context, Iterable<Notification> messages,
          Collector<Notification> collector) throws Exception {

        var allText = StreamSupport.stream(messages.spliterator(), false)
              .map(sm -> sm.getTitle() + "\n" + sm.getMessage())
              .collect(Collectors.joining("\n---\n"));

        if (!allText.isBlank()) {

            var nt = new Notification();

            nt.setTitle("/\\/ messages");
            nt.setMessage(allText);

            collector.collect(nt);
        }
    }
}
