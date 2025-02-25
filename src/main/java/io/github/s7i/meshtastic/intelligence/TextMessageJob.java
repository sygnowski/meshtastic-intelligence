package io.github.s7i.meshtastic.intelligence;

import com.geeksville.mesh.MeshProtos.FromRadio;
import com.geeksville.mesh.MeshProtos.FromRadio.PayloadVariantCase;
import com.geeksville.mesh.Portnums.PortNum;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TextMessageJob extends JobStub {

    public TextMessageJob(ParameterTool params, StreamExecutionEnvironment env, Configuration cfg) {
        super(params, env, cfg);
    }

    @Override    public void build() {


        fromKafkaSource()
              .filter(p -> {
                  var fromRadio = FromRadio.parseFrom(p.payload());
                  return fromRadio.getPayloadVariantCase() == PayloadVariantCase.PACKET &&
                        fromRadio.getPacket().getDecoded().getPortnum() == PortNum.TEXT_MESSAGE_APP;
              })
              .keyBy(p -> FromRadio.parseFrom(p.payload()).getPacket().getFrom() & 0xffffffffL )
              .process(new TextAppProcessor())
              .uid("text-app")
              .name("Text App Processor")
              .print();

    }
}
