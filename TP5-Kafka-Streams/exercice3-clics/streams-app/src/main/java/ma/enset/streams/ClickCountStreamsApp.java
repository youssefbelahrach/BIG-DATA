package ma.enset.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * TP5 - Exercice 3 - Partie 2 : Application Kafka Streams.
 */
public class ClickCountStreamsApp {

    private static final String TOPIC_IN = "clicks";
    private static final String TOPIC_OUT = "click-counts";

    public static void main(String[] args) {
        // Variante choisie : "global" ou "user" (par défaut : user)
        String variante = (args.length > 0) ? args[0] : "user";
        System.out.println(">>> Variante de comptage : " + variante);

        // 1. Configuration de l'application Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "click-count-app-" + variante);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 2. Construction de la topologie
        StreamsBuilder builder = new StreamsBuilder();

        // KStream : flux continu d'événements de clics (clé = userId, valeur = "click")
        KStream<String, String> clicks = builder.stream(
                TOPIC_IN,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KGroupedStream<String, String> grouped;

        if ("global".equalsIgnoreCase(variante)) {
            // Variante 1 : comptage global.
            // On remplace toutes les clés par la clé unique "total"
            // afin que tous les clics soient comptés ensemble.
            grouped = clicks
                    .map((key, value) -> KeyValue.pair("total", value))
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        } else {
            // Variante 2 : comptage par utilisateur.
            // La clé du message est déjà le userId : on groupe directement par clé.
            grouped = clicks.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        }

        // KTable : résultat de l'agrégation count(), continuellement mis à jour
        KTable<String, Long> counts = grouped.count(
                Materialized.as("click-counts-store")
        );

        // Publication des résultats dans le topic de sortie.
        // La valeur Long est convertie en String pour rester lisible
        // avec kafka-console-consumer et le consommateur REST.
        counts.toStream()
              .peek((key, count) -> System.out.println("Comptage mis à jour -> " + key + " = " + count))
              .mapValues(count -> String.valueOf(count))
              .to(TOPIC_OUT, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        System.out.println(topology.describe());

        // 3. Démarrage de l'application
        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        // Hook d'arrêt propre (Ctrl+C) : ferme correctement l'application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println(">>> Arrêt de l'application Kafka Streams...");
            streams.close();
            latch.countDown();
        }, "streams-shutdown-hook"));

        try {
            streams.start();
            System.out.println(">>> Application Kafka Streams démarrée (topic " + TOPIC_IN
                    + " -> topic " + TOPIC_OUT + ")");
            latch.await();
        } catch (Exception e) {
            System.err.println("Erreur : " + e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
