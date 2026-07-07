package ma.enset.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Contrôleur qui reçoit les clics de la page web et les publie dans Kafka.
 *
 * Message envoyé dans le topic "clicks" :
 *   - clé   : l'identifiant de l'utilisateur (ex : user1)
 *   - valeur : l'action (ex : click)
 */
@RestController
public class ClickController {

    private static final Logger log = LoggerFactory.getLogger(ClickController.class);
    private static final String TOPIC_CLICKS = "clicks";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ClickController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Appelé par le bouton de la page web.
     * Exemple : POST /click?userId=user1
     */
    @PostMapping("/click")
    public Map<String, String> click(@RequestParam(defaultValue = "user1") String userId) {
        String key = userId;
        String value = "click";

        kafkaTemplate.send(TOPIC_CLICKS, key, value);
        log.info("Événement envoyé -> topic={}, key={}, value={}", TOPIC_CLICKS, key, value);

        return Map.of(
                "status", "ok",
                "topic", TOPIC_CLICKS,
                "key", key,
                "value", value
        );
    }
}
