package uk.tommyt.kafka.replay;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class EventConsumer {
    private final Consumer<byte[], byte[]> consumer;
    private final Output sink;
    private String topic;

    EventConsumer(Properties properties, Output sink, String topic) {
        this.sink = sink;
        this.topic = topic;
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumer = new KafkaConsumer<>(properties);
    }

    public void poll() {
        consumer.assign(allTPFor(topic));
        consumer.seekToBeginning(allTPFor(topic));

        while (!Thread.interrupted()) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000L);
            records.forEach(sink::addRecord);
        }

        consumer.close();
    }

    private Collection<TopicPartition> allTPFor(String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(t -> new TopicPartition(t.topic(), t.partition()))
                .collect(Collectors.toSet());
    }

}
