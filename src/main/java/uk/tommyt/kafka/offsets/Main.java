package uk.tommyt.kafka.offsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final Properties properties = new Properties();

    public static void main(String[] args) {
        try {
            if (args.length != 1) {
                throw new IllegalArgumentException("Need properties file as argument");
            }

            Long period;
            try {
                File consumerPropertiesFile = new File(args[0]);
                properties.load(new FileInputStream(consumerPropertiesFile));
                spark.Spark.port(Integer.parseInt(propertyFor("http.port").orElse("8080")));
                period = Long.parseLong(propertyFor("poll.period").orElse("10000"));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Was not a number: %s", propertyFor("http.port")));
            } catch (IOException e) {
                throw new IllegalArgumentException("Opening configuration", e);
            }

            new Poller(propertiesFor("consumer"), Offsets.INSTANCE, period);
            LOG.debug("Poller started");

        } catch (IllegalArgumentException iae) {
            LOG.error("Configuration issue", iae);
        }
    }

    private static Properties propertiesFor(String prefix) {
        Properties newProperties = new Properties();
        properties.keySet().stream()
                .filter(k -> k.toString().startsWith(prefix + "."))
                .forEach(k -> newProperties.setProperty(
                        k.toString().substring(prefix.length() + 1), properties.getProperty(k.toString())));
        return newProperties;
    }

    private static Optional<String> propertyFor(String key) {
        return Optional.ofNullable(properties.getProperty(key));
    }
}