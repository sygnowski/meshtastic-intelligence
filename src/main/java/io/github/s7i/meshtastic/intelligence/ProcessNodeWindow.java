package io.github.s7i.meshtastic.intelligence;

import io.github.s7i.meshtastic.intelligence.Model.NodeContext;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class ProcessNodeWindow extends ProcessWindowFunction<Row, Row, Row, TimeWindow> {

    @Override
    public void process(Row key, ProcessWindowFunction<Row, Row, Row, TimeWindow>.Context context, Iterable<Row> elements, Collector<Row> out)
          throws Exception {

        var startTime = context.window().getStart();
        var endTime = context.window().getEnd();
        var count = StreamSupport.stream(elements.spliterator(), false)
              .count();
        var nodeContext = NodeContext.from(key);
        var outRow = Row.withNames();

        outRow.setField("from", nodeContext.getFromNode());
        outRow.setField("to", nodeContext.getToNode());
        outRow.setField("count", (int) count);
        outRow.setField("start", startTime);
        outRow.setField("end", endTime);
        outRow.setField("kind", nodeContext.getKind());
        outRow.setField("jobId", getRuntimeContext().getJobId().toHexString());

        out.collect(outRow);
    }
}
