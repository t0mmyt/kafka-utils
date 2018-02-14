package uk.tommyt.kafka.offsets;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

class Poller {
    private final Offsets offsets;
    private final Consumer<byte[], byte[]> consumer;

    Poller(Properties consumerProperties, Offsets offsets, long period) {
        this.offsets = offsets;
        consumer = new KafkaConsumer<>(consumerProperties);
        assignAllTopics();
        Timer timer = new Timer();
        timer.schedule(new Poll(), 0L, period);
    }

    private class Poll extends TimerTask {

        @Override
        public void run() {
            updateEndOffsets();
        }
    }

    private void updateEndOffsets() {
        consumer.endOffsets(allTopics()).forEach((t, o) -> offsets.updateTopic(t.topic(), t.partition(), o));
    }

    private void assignAllTopics() {
        consumer.assign(allTopics());
    }

    private Set<TopicPartition> allTopics() {
        return consumer.listTopics().values().stream()
                .flatMap(Collection::stream)
                .map(t -> new TopicPartition(t.topic(), t.partition()))
                .collect(Collectors.toSet());
    }
}