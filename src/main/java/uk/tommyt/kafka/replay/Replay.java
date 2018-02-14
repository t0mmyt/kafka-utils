package uk.tommyt.kafka.replay;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Replay {
    private static final Logger LOG = LoggerFactory.getLogger(Replay.class);
    private static final Properties properties = new Properties();

    public static void main(String[] args) throws SQLException {
        Connection connection;

        if (args.length != 1) {
            throw new IllegalArgumentException("Need properties file as argument");
        }

        try {
            File consumerPropertiesFile = new File(args[0]);
            properties.load(new FileInputStream(consumerPropertiesFile));
        } catch (IOException e) {
            throw new IllegalArgumentException("Opening configuration", e);
        }

        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:/tmp/dump.db");
        } catch (ClassNotFoundException e) {
            LOG.error("Could not load class", e);
            return;
        }

        Output output = new Output(connection);
        EventConsumer consumer = new EventConsumer(propertiesFor("consumer"), output, "TRX-3-PDC-ADM");
        consumer.poll();
    }

    private static Properties propertiesFor(String prefix) {
        Properties newProperties = new Properties();
        properties.keySet().stream()
                .filter(k -> k.toString().startsWith(prefix + "."))
                .forEach(k -> newProperties.setProperty(
                        k.toString().substring(prefix.length() + 1), properties.getProperty(k.toString())));
        return newProperties;
    }
}
