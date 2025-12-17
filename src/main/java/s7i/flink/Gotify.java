package s7i.flink;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

@Slf4j
public class Gotify {

    private final String uri;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    public Gotify(String uri) {
        this.uri = uri;

        objectMapper = new ObjectMapper()
              .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
              .configure(SerializationFeature.INDENT_OUTPUT, true);

        httpClient = HttpClient.newBuilder()
              .connectTimeout(Duration.ofSeconds(60))
              .build();
    }

    public void notify(String title, String message) {
        try {
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

                httpClient.sendAsync(request, BodyHandlers.ofString())
                      .whenComplete((st, th) -> {
                          if (th != null) {
                              log.warn("gotify", th);
                          } else {
                              int statusCode = st.statusCode();
                              if (statusCode != 200) {
                                  log.warn("gotify status code: {}", statusCode);
                              }
                          }
                      });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
 }
