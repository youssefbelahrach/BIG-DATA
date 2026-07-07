package ma.enset.tp5.ex2;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Exercice 2 : Analyse de données météorologiques.
 */
public class WeatherApp {

    private static final String INPUT_TOPIC = "weather-data";
    private static final String OUTPUT_TOPIC = "station-averages";
    private static final double TEMPERATURE_THRESHOLD_C = 30.0;

    /** Représente une mesure météo valide. */
    record WeatherReading(String station, double temperature, double humidity) {}

    public static void main(String[] args) throws InterruptedException {

        // 1. Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analytics-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 2. Topologie
        StreamsBuilder builder = new StreamsBuilder();

        // KStream : flux brut de lignes CSV "station,temperature,humidity"
        KStream<String, String> rawStream = builder.stream(
                INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Parsing : une ligne mal formée (ex : "Station1,error,60")
        KStream<String, WeatherReading> readings = rawStream
                .flatMapValues(WeatherApp::parse);

        // Filtrage : on conserve uniquement les relevés > 30 °C
        // puis conversion Celsius -> Fahrenheit : F = C * 9/5 + 32
        KStream<String, WeatherReading> hotInFahrenheit = readings
                .filter((key, r) -> r.temperature() > TEMPERATURE_THRESHOLD_C)
                .mapValues(r -> new WeatherReading(
                        r.station(),
                        r.temperature() * 9.0 / 5.0 + 32.0,
                        r.humidity()))
                .peek((k, r) -> System.out.printf(Locale.US,
                        "[FILTRÉ+CONVERTI] %s -> %.1f F, %.1f %%%n",
                        r.station(), r.temperature(), r.humidity()));

        // Regroupement par station : la clé devient le nom de la station.
        // L'accumulateur est encodé en String "count,sumTemperature,sumHumidity"
        // pour rester sur des Serdes String simples.
        KGroupedStream<String, String> groupedByStation = hotInFahrenheit
                .map((key, r) -> KeyValue.pair(
                        r.station(),
                        r.temperature() + "," + r.humidity()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        // Agrégation : le résultat est une KTable (vue mise à jour en continu)
        KTable<String, String> aggregates = groupedByStation.aggregate(
                () -> "0,0.0,0.0",                       // initialisation
                (station, newValue, aggregate) -> {       // accumulation
                    String[] agg = aggregate.split(",");
                    long count = Long.parseLong(agg[0]) + 1;
                    double sumTemp = Double.parseDouble(agg[1]);
                    double sumHum = Double.parseDouble(agg[2]);

                    String[] parts = newValue.split(",");
                    sumTemp += Double.parseDouble(parts[0]);
                    sumHum += Double.parseDouble(parts[1]);

                    return count + "," + sumTemp + "," + sumHum;
                },
                Materialized.with(Serdes.String(), Serdes.String()));

        // Calcul des moyennes et publication dans le topic de sortie
        aggregates.toStream()
                .mapValues((station, aggregate) -> {
                    String[] agg = aggregate.split(",");
                    long count = Long.parseLong(agg[0]);
                    double avgTemp = Double.parseDouble(agg[1]) / count;
                    double avgHum = Double.parseDouble(agg[2]) / count;
                    return String.format(Locale.US,
                            "Temperature moyenne = %.1f F, Humidite moyenne = %.1f %%",
                            avgTemp, avgHum);
                })
                .peek((station, v) -> System.out.println("[RÉSULTAT] " + station + " : " + v))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 3. Démarrage + hook d'arrêt propre
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }, "shutdown-hook"));

        streams.start();
        System.out.println("Application WeatherApp démarrée. Ctrl+C pour arrêter.");
        latch.await();
    }

    /**
     * Transforme une ligne CSV en WeatherReading.
     * Retourne une liste vide si la ligne est mal formée,
     * afin que le flux continue sans interruption.
     */
    private static List<WeatherReading> parse(String line) {
        try {
            String[] parts = line.split(",");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Nombre de champs incorrect");
            }
            String station = parts[0].trim();
            double temperature = Double.parseDouble(parts[1].trim());
            double humidity = Double.parseDouble(parts[2].trim());
            if (station.isEmpty()) {
                throw new IllegalArgumentException("Station vide");
            }
            return List.of(new WeatherReading(station, temperature, humidity));
        } catch (Exception e) {
            System.err.println("[MAL FORMÉ ignoré] \"" + line + "\" : " + e.getMessage());
            return Collections.emptyList();
        }
    }
}
