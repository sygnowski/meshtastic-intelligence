package s7i.flink.meshcore;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import s7i.flink.io.Packet;

@Slf4j
public class ProcessDecoded extends ProcessFunction<Packet, TextMessage> {

    private transient TextMessageMapper mapper;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        mapper = new TextMessageMapper();
    }

    @Override
    public void processElement(Packet packet, ProcessFunction<Packet, TextMessage>.Context context, Collector<TextMessage> collector) throws Exception {

        mapper.map(packet).ifPresent(collector::collect);
    }
}
