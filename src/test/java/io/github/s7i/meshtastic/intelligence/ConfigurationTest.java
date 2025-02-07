package io.github.s7i.meshtastic.intelligence;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.s7i.meshtastic.intelligence.Configuration.Topic;
import org.junit.jupiter.api.Test;

class ConfigurationTest {


    @Test
    void load_configuration() {
        Configuration c = Configuration.from("src/test/resources/conf.yaml");

        assertNotNull(c);

        var k = c.getKafka();

        assertTrue(() -> k.contains("aaa:bbb"));
        assertTrue(() -> k.contains("ccc:ddd"));

        assertTrue(() -> c.getTopics()
              .stream()
              .filter(t -> t.getTag().equals("source"))
              .map(Topic::getName)
              .anyMatch("meshtastic-from-radio"::equals));

    }
}