package ma.enset.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * TP5 - Exercice 3 - Partie 1 : Producteur Web.
 * Application Spring Boot qui affiche un bouton et envoie un événement
 * dans le topic Kafka "clicks" à chaque clic.
 */
@SpringBootApplication
public class ProducteurWebApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducteurWebApplication.class, args);
    }
}
