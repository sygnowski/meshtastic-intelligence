package io.github.s7i.meshtastic.intelligence;

import static java.util.Objects.requireNonNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Slf4j
@NoArgsConstructor
@Data
public class Configuration {

    @SneakyThrows
    public static Configuration from(String path) {
        log.debug("reading configuration from path: {}", requireNonNull(path));

        LoaderOptions loadingConfig = new LoaderOptions();
        Yaml yaml = new Yaml(new Constructor(Configuration.class, loadingConfig));

        try (var reader = Files.newBufferedReader(Path.of(path))) {
            return yaml.load(reader);
        }
    }

    @Data
    @NoArgsConstructor
    public static class Topic {

        private String name;
        private String tag;
    }

    private String name;
    private List<String> kafka;
    private List<Topic> topics;


    public String getTopic(String tag) {
        return getTopics()
              .stream()
              .filter(t -> t.getTag().equals(tag))
              .map(Topic::getName)
              .findFirst()
              .orElseThrow();
    }

}
