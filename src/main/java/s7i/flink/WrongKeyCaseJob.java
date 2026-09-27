package s7i.flink;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class WrongKeyCaseJob  {

    public static final ValueStateDescriptor<Long> STATE_DESCRIPTOR = new ValueStateDescriptor<>("acc", TypeInformation.of(Long.class));


    private final StreamExecutionEnvironment env;
    public void build() {

        TypeInformation<Row> rowType = Types.ROW_NAMED(
                new String[]{"id", "value"},
                Types.STRING,
                Types.LONG
        );

        TypeInformation<Row> rowTypeTotal = Types.ROW_NAMED(
                new String[]{"total"},
                Types.LONG
        );

        var row = Row.withNames();
        row.setField("id", "my-id-01");
        row.setField("value", 10L);

        var row2 = Row.withNames();
        row2.setField("id", "my-id-01");
        row2.setField("value", 20L);


        var stream = env.fromCollection(List.of(row, row2), rowType);
        stream.keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        return "static-key";
                    }
                })
                .process(new KeyedProcessFunction<String, Row, Row>() {
                    @Override
                    public void processElement(Row row, KeyedProcessFunction<String, Row, Row>.Context context, Collector<Row> collector) throws Exception {
                        collector.collect(row);
                    }
                }).returns(rowType)
                .uid("pf01")
                .name("pf01").keyBy(new KeySelector<Row, String>() {

                    int noOfCall;

                    @Override
                    public String getKey(Row row) throws Exception {
                        String newKey = row.getFieldAs("id");
                        newKey += "#"+ ++noOfCall; // THE BUG

                        log.info("get key: {}", newKey);
                        return newKey;
                    }
                }).process(new KeyedProcessFunction<String, Row, Row>() {

                    ValueState<Long> stateAccu;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        super.open(parameters);

                        stateAccu = getRuntimeContext().getState(STATE_DESCRIPTOR);
                    }

                    @Override
                    public void processElement(Row row, KeyedProcessFunction<String, Row, Row>.Context context, Collector<Row> collector) throws Exception {
                        long actual = row.getFieldAs("value");
                        Long old = stateAccu.value();
                        if (old == null) {
                            stateAccu.update(row.getFieldAs("value"));
                        } else {

                            actual += old;

                            stateAccu.update(actual);
                        }

                        var total = Row.withNames();
                        total.setField("total", actual);

                        collector.collect(total);

                    }
                }).returns(rowTypeTotal).uid("pf-accu").name("pf-accu").setParallelism(5)
                .addSink(new SinkFunction<Row>() {
                    @Override
                    public void invoke(Row row, Context context) {
                        log.info("Row: {}", row);
                    }
                });

        try {
            env.execute("WrongKeyCase");
        } catch (Exception e) {
            log.error("running job", e);
        }

    }

    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        new WrongKeyCaseJob(env).build();
    }
}
