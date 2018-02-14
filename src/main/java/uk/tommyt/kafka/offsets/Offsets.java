package uk.tommyt.kafka.offsets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.util.Map;
import java.util.TreeMap;

public enum Offsets {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(Offsets.class);
    private Map<String, Map<Integer, Long>> metrics = new TreeMap<>();
    private ObjectMapper objectMapper = new ObjectMapper();

    public void updateTopic(String topic, int partition, long offset) {
        if (!metrics.containsKey(topic)) {
            metrics.put(topic, new TreeMap<>());
        }
        metrics.get(topic).put(partition, offset);
    }

    // HTTP Endpoints
    {
        spark.Spark.get("/topics", this::perTopic);
        spark.Spark.get("/topics/:topic", this::perTopic);
        spark.Spark.get("/partitions", this::perPartition);
    }

    private String perPartition(Request request, Response response) throws JsonProcessingException {
        return objectMapper.writeValueAsString(metrics);
    }

    private String perTopic(Request request, Response response) {
        ObjectNode node = objectMapper.createObjectNode();
        if (request.params().containsKey(":topic")) {
            if (metrics.containsKey(request.params(":topic"))) {
                metrics.get(request.params("topic")).forEach((k, v) -> node.put(k.toString(), v));
            } else {
                response.status(404);
                return "Topic not found";
            }
        } else {
            metrics.keySet().forEach(k -> node.put(k, metrics.get(k).values().stream().mapToLong(Number::longValue).sum()));
        }
        return node.toString();
    }

}