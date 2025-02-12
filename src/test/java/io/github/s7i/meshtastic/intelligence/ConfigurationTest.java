package io.github.s7i.meshtastic.intelligence;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import org.junit.jupiter.api.Test;

class ConfigurationTest {


    @Test
    void load_configuration() {
        Configuration c = Configuration.from("src/test/resources/conf.yaml");

        assertNotNull(c);

        var t = c.getTopic("source");

        var k = t.getKafka();

        assertTrue(() -> k.contains("bootstrap.servers=kafka123"));
        assertTrue(() -> k.contains("client.id=1231"));

        assertTrue(() -> "meshtastic-from-radio".equals(t.getName()));

    }
}