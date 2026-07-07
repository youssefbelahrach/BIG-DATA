package ma.enset.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * TP5 - Exercice 3 - Partie 3 : Consommateur REST.
 * Consomme les résultats du topic "click-counts" et les expose
 * via l'API REST : GET /clicks/count
 */
@SpringBootApplication
public class ConsommateurRestApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsommateurRestApplication.class, args);
    }
}
