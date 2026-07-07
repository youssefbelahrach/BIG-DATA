package ma.enset.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consommateur Kafka qui écoute le topic "click-counts".
 */
@Component
public class ClickCountListener {

    private static final Logger log = LoggerFactory.getLogger(ClickCountListener.class);

    /** Derniers comptages connus, par clé. */
    private final Map<String, Long> counts = new ConcurrentHashMap<>();

    @KafkaListener(topics = "click-counts", groupId = "click-counts-rest")
    public void onClickCount(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        if (key == null || value == null) {
            log.warn("Message ignoré (clé ou valeur nulle) : {}", record);
            return;
        }

        try {
            long count = Long.parseLong(value.trim());
            counts.put(key, count);
            log.info("Comptage reçu -> {} = {}", key, count);
        } catch (NumberFormatException e) {
            log.warn("Message mal formé ignoré : key={}, value={}", key, value);
        }
    }

    /** Retourne une copie des comptages par clé. */
    public Map<String, Long> getCounts() {
        return Map.copyOf(counts);
    }

    /** Retourne le nombre total de clics (somme de toutes les clés). */
    public long getTotal() {
        return counts.values().stream().mapToLong(Long::longValue).sum();
    }
}
