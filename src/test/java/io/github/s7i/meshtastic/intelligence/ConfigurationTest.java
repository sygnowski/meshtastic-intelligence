package io.github.s7i.meshtastic.intelligence;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    void config_with_resolved_value() {
        Configuration c = Configuration.from("src/test/resources/conf.yaml");

        assertEquals("my-password-is-secret-password", c.getOption("value-with-password"));

    }

    @Test
    void config_with_resolved_value_throw() {
        Configuration c = Configuration.from("src/test/resources/conf.yaml");

        assertThrows(IllegalStateException.class, () -> c.getOption("value-with-missing-expression"));

    }
}