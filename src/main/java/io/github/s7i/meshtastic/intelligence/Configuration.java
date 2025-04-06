package io.github.s7i.meshtastic.intelligence;

import static java.util.Objects.requireNonNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookup;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Slf4j
@Data
public class Configuration {

    private class Resolver implements StringLookup {
        private final StringSubstitutor sysSubstitutor = StringSubstitutor.createInterpolator().setEnableSubstitutionInVariables(true);
        private final StringSubstitutor substitutor = new StringSubstitutor(this).setEnableSubstitutionInVariables(true);


        @Override
        public String lookup(String key) {
            return Configuration.this.findOption(key)
                    .orElseThrow(() -> new IllegalStateException("can't resolve: " + key));
        }
        public String resolve(String input) {
            return sysSubstitutor.replace(substitutor.replace(input));
        }
    }

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
        private String kafka;
    }

    @Data
    public static class Option {

        private String name;
        private String value;
    }

    public Configuration() {

    }

    private String name;
    private List<Topic> topics;
    private List<Option> options;
    private final Resolver resolver = new Resolver();

    public Topic getTopic(String tag) {
        return getTopics()
              .stream()
              .filter(t -> t.getTag().equals(tag))
              .findFirst()
              .orElseThrow(() -> new RuntimeException("missing config option:" + tag));
    }

    public Optional<String> findOption(String optionName) {
        return getOptions()
              .stream()
              .filter(o -> o.getName().equals(optionName))
              .map(Option::getValue)
              .map(resolver::resolve)
              .findFirst();
    }

    public String getOption(String optionName) {
        return findOption(optionName)
              .orElseThrow(() -> new RuntimeException("missing config option:" + optionName));
    }

    public String getOption(String optionName, String defaultValue) {
        return findOption(optionName)
              .orElse(defaultValue);
    }

    public void onTrue(String optionName, Runnable iftrue) {
        if (Boolean.parseBoolean(getOption(optionName, ""))) {
            iftrue.run();
        } else {
            log.debug("no action on option: {}", optionName);
        }
    }
}
