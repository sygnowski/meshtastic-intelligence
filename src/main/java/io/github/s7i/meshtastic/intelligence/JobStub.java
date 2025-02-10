package io.github.s7i.meshtastic.intelligence;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@RequiredArgsConstructor
public abstract class JobStub {

    protected final ParameterTool params;
    protected final StreamExecutionEnvironment env;
    protected final Configuration cfg;

    public abstract void build();
}
