package io.github.s7i.meshtastic.intelligence;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GitProps {

    public static final String GIT_PROPERTIES = "/git.properties";

    private static class Holder {

        private static final GitProps instance = new GitProps();
    }

    public static GitProps get() {
        return Holder.instance;
    }

    private final Properties props;

    private GitProps() {
        props = new Properties();
        try (var is = GitProps.class.getResourceAsStream(GIT_PROPERTIES)) {
            props.load(is);
        } catch (Exception io) {
            log.error("loading git properties", io);
        }
    }

    @Override
    public String toString() {
        var branch = props.getProperty("git.branch", "");
        var commit = props.getProperty("git.commit.id.abbrev", "");
        return String.format("%s | %s", branch, commit);
    }
}
