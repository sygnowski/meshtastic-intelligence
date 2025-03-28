package io.github.s7i.meshtastic.intelligence;

import static java.util.Objects.requireNonNull;

import io.github.s7i.meshtastic.intelligence.Model.TextMessage;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

@Slf4j
public class TextMessageNotification extends RichSinkFunction<Row> {


    public static final String TEXT_HTTP_URI = "text.http.uri";
    private transient ExecutorService executor;
    private transient HttpClient httpClient;

    private transient String uri;
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        uri = requireNonNull(getRuntimeContext()
              .getExecutionConfig()
              .getGlobalJobParameters()
              .toMap()
              .get(TEXT_HTTP_URI), TEXT_HTTP_URI);

        objectMapper = new ObjectMapper()
              .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
              .configure(SerializationFeature.INDENT_OUTPUT, true);

        httpClient = HttpClient.newBuilder()
              .connectTimeout(Duration.ofSeconds(60))
              .build();

        executor = Executors.newFixedThreadPool(1, run -> new Thread(run, "text-message-thread"));
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        executor.submit(() -> {
            var title = String.format("Message from node: (%s) on channel: (%s)",
                  value.getField(TextMessage.FROM_NODE),
                  value.getField(TextMessage.CHANNEL)
            );
            var message = String.format("Time: %s\nText: %s",
                  value.getField(TextMessage.TIME),
                  value.getField(TextMessage.TEXT)
            );

            var jsonText = objectMapper.writeValueAsString(Map.of(
                  "title", title,
                  "message", message,
                  "priority", 2)
            );

            log.debug("Sending payload: {}", jsonText);

            var request = HttpRequest.newBuilder()
                  .uri(URI.create(uri))
                  .header("Content-Type", "application/json")
                  .POST(BodyPublishers.ofString(jsonText))
                  .build();

            try {
                var hnd = httpClient.send(request, BodyHandlers.ofString());
                int statusCode = hnd.statusCode();
                return statusCode;
            } catch (IOException e) {
                log.error("Http request failed", e);
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
                Thread.currentThread().interrupt();
            }
            return 0;
        }).get(60, TimeUnit.SECONDS);
    }
}
