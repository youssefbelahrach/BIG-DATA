package ma.enset.tp5.ex1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Exercice 1 : Nettoyage et validation de messages texte.
 */
public class TextCleaningApp {

    // Topics
    private static final String INPUT_TOPIC = "text-input";
    private static final String CLEAN_TOPIC = "text-clean";
    private static final String DEAD_LETTER_TOPIC = "text-dead-letter";

    // Règles de validation
    private static final int MAX_LENGTH = 100;
    private static final List<String> FORBIDDEN_WORDS = List.of("HACK", "SPAM", "XXX");

    public static void main(String[] args) throws InterruptedException {

        // 1. Configuration de l'application Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-cleaning-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 2. Construction de la topologie
        StreamsBuilder builder = new StreamsBuilder();

        // Lecture du topic d'entrée sous forme de KStream<String, String>
        KStream<String, String> input = builder.stream(
                INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Étape de nettoyage :
        //  - trim         : suppression des espaces au début et à la fin
        //  - \\s+ -> " "  : remplacement des espaces multiples par un seul espace
        //  - toUpperCase  : conversion en majuscules
        KStream<String, String> cleaned = input.mapValues(value -> {
            if (value == null) {
                return "";
            }
            return value.trim().replaceAll("\\s+", " ").toUpperCase();
        });

        // Séparation du flux en deux branches : valides / invalides
        cleaned.split()
                .branch((key, value) -> isValid(value),
                        Branched.withConsumer(valid ->
                                valid.peek((k, v) -> System.out.println("[VALIDE]   -> " + v))
                                     .to(CLEAN_TOPIC, Produced.with(Serdes.String(), Serdes.String()))))
                .defaultBranch(
                        Branched.withConsumer(invalid ->
                                invalid.peek((k, v) -> System.out.println("[REJETÉ]   -> " + v))
                                       .to(DEAD_LETTER_TOPIC, Produced.with(Serdes.String(), Serdes.String()))));

        // 3. Démarrage de l'application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);

        // Hook d'arrêt propre (Ctrl+C)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }, "shutdown-hook"));

        streams.start();
        System.out.println("Application TextCleaningApp démarrée. Ctrl+C pour arrêter.");
        latch.await();
    }

    /**
     * Un message est invalide si :
     *  - il est vide ou ne contient que des espaces ;
     *  - il contient un mot interdit (HACK, SPAM, XXX) ;
     */
    private static boolean isValid(String message) {
        if (message == null || message.isBlank()) {
            return false;
        }
        if (message.length() > MAX_LENGTH) {
            return false;
        }
        for (String forbidden : FORBIDDEN_WORDS) {
            if (message.contains(forbidden)) {
                return false;
            }
        }
        return true;
    }
}
