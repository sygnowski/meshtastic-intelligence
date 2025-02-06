package io.github.s7i.meshtastic.intelligence;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class IntelligenceJob {


    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            DataStream<Row> dataStream = env.fromElements(
                  Row.of("Alice", 12),
                  Row.of("Bob", 10),
                  Row.of("Alice", 100));


            Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");

            tableEnv.createTemporaryView("InputTable", inputTable);
            Table resultTable = tableEnv.sqlQuery(
                  "SELECT name, SUM(score) FROM InputTable GROUP BY name");

            DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

            resultStream.print();
            env.execute();
        } catch (Exception e) {
            log.error("oops", e);
        }
    }
}
