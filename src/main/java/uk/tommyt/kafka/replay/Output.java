package uk.tommyt.kafka.replay;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class Output {
    private static final Logger LOG = LoggerFactory.getLogger(Output.class);
    private static final String createSql = "CREATE TABLE IF NOT EXISTS kafka_dump (key BLOB, value BLOB);";
    private static final String insertSql = "INSERT INTO kafka_dump (key, value) VALUES (? ,?);";
    private final Connection connection;

    Output(Connection connection) throws SQLException {
        this.connection = connection;
        LOG.info("Ensuring table");
        Statement statement = connection.createStatement();
        statement.execute(createSql);
        statement.close();
    }

    public boolean addRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
        try {
            LOG.trace("Writing record");
            PreparedStatement statement = connection.prepareStatement(insertSql);
            statement.setBytes(1, consumerRecord.key());
            statement.setBytes(2, consumerRecord.value());
            return statement.execute();
        } catch (SQLException e) {
            LOG.error("Error writing record", e);
            return false;
        }
    }
}
