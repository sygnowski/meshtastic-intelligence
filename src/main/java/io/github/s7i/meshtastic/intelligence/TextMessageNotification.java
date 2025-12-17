package io.github.s7i.meshtastic.intelligence;

import static java.util.Objects.requireNonNull;

import io.github.s7i.meshtastic.intelligence.Model.Notification;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import s7i.flink.Gotify;

@Slf4j
public class TextMessageNotification extends RichSinkFunction<Notification> {


    public static final String TEXT_HTTP_URI = "text.http.uri";
    private transient ExecutorService executor;
    private transient Gotify gotify;

    private transient String uri;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        uri = requireNonNull(getRuntimeContext()
              .getExecutionConfig()
              .getGlobalJobParameters()
              .toMap()
              .get(TEXT_HTTP_URI), TEXT_HTTP_URI);
        executor = Executors.newFixedThreadPool(1, run -> new Thread(run, "text-message-thread"));
        gotify = new Gotify(uri);
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
    }

    @Override
    public void invoke(Notification value, Context context) throws Exception {
        executor.submit(() -> gotify.notify(value.getTitle(), value.getMessage()));
    }
}
