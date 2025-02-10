package io.github.s7i.meshtastic.intelligence;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class ProcessNodeWindow extends ProcessWindowFunction<Row, Row, String, TimeWindow> {

    @Override
    public void process(String s, ProcessWindowFunction<Row, Row, String, TimeWindow>.Context context, Iterable<Row> elements, Collector<Row> out)
          throws Exception {

        var accu = new Accumulator();

        for (var e : elements) {
            accu.incCount(e);
        }

        long startTime = context.window().getStart();
        long endTime = context.window().getEnd();

        for (var e : accu.getNodeCount().entrySet()) {
            out.collect(Row.of(
                  e.getKey().getFromNode(),
                  e.getKey().getToNode(),
                  e.getValue(),
                  startTime,
                  endTime
                  )
            );
        }
    }
}
