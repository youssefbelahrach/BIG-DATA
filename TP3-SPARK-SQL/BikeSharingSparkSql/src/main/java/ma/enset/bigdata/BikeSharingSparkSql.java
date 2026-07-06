package ma.enset.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

/**
 * Solution du TP Spark SQL - analyse d'un service public de location de velos.
 *
 * L'application execute exclusivement les analyses demandees avec Spark SQL
 * apres avoir charge, nettoye et type le fichier CSV dans un DataFrame.
 */
public final class BikeSharingSparkSql {

    private static final String DEFAULT_INPUT_PATH = "data/bike_sharing.csv";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final int DISPLAY_ROWS = 20;

    private BikeSharingSparkSql() {
        // Classe utilitaire : aucune instance necessaire.
    }

    public static void main(String[] args) {
        System.out.println("Java utilisée : " + System.getProperty("java.version"));

        String inputPath = args.length > 0 ? args[0] : DEFAULT_INPUT_PATH;

        SparkSession spark = SparkSession.builder()
                .appName("Bike Sharing Spark SQL - Java")
                // Cette valeur est utilisee seulement lors d'une execution locale.
                // spark-submit peut la remplacer avec --master.
                .config("spark.master", System.getProperty("spark.master", "local[*]"))
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<Row> rentals = loadAndPrepareData(spark, inputPath);

            printSection("1. Chargement et exploration des donnees");
            System.out.println("Schema du DataFrame :");
            rentals.printSchema();

            System.out.println("Les 5 premieres lignes :");
            rentals.show(5, false);

            System.out.printf("Nombre total de locations : %d%n", rentals.count());

            // Vue SQL temporaire demandee par l'enonce.
            rentals.createOrReplaceTempView("bike_rentals_view");
            System.out.println("Vue temporaire creee : bike_rentals_view");

            printSection("3. Requetes SQL de base");
            showQuery(spark,
                    "Locations dont la duree est superieure a 30 minutes (20 premieres affichees)",
                    "SELECT * FROM bike_rentals_view " +
                            "WHERE duration_minutes > 30 " +
                            "ORDER BY duration_minutes DESC, rental_id", DISPLAY_ROWS);

            showQuery(spark,
                    "Locations commencant a Station A",
                    "SELECT * FROM bike_rentals_view " +
                            "WHERE start_station = 'Station A' " +
                            "ORDER BY start_time", DISPLAY_ROWS);

            showQuery(spark,
                    "Chiffre d'affaires total",
                    "SELECT ROUND(SUM(price), 2) AS total_revenue " +
                            "FROM bike_rentals_view", DISPLAY_ROWS);

            printSection("4. Requetes d'agregation");
            showQuery(spark,
                    "Nombre de locations par station de depart",
                    "SELECT start_station, COUNT(*) AS rental_count " +
                            "FROM bike_rentals_view " +
                            "GROUP BY start_station " +
                            "ORDER BY rental_count DESC, start_station", DISPLAY_ROWS);

            showQuery(spark,
                    "Duree moyenne de location par station de depart",
                    "SELECT start_station, ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes " +
                            "FROM bike_rentals_view " +
                            "GROUP BY start_station " +
                            "ORDER BY avg_duration_minutes DESC, start_station", DISPLAY_ROWS);

            showQuery(spark,
                    "Station ayant le plus grand nombre de locations",
                    "SELECT start_station, COUNT(*) AS rental_count " +
                            "FROM bike_rentals_view " +
                            "GROUP BY start_station " +
                            "ORDER BY rental_count DESC, start_station " +
                            "LIMIT 1", DISPLAY_ROWS);

            printSection("5. Analyse temporelle");
            showQuery(spark,
                    "Heure extraite de chaque date de depart (20 premieres lignes affichees)",
                    "SELECT rental_id, start_time, HOUR(start_time) AS start_hour " +
                            "FROM bike_rentals_view " +
                            "ORDER BY rental_id", DISPLAY_ROWS);

            showQuery(spark,
                    "Nombre de locations par heure",
                    "SELECT HOUR(start_time) AS start_hour, COUNT(*) AS rental_count " +
                            "FROM bike_rentals_view " +
                            "GROUP BY HOUR(start_time) " +
                            "ORDER BY rental_count DESC, start_hour", DISPLAY_ROWS);

            showQuery(spark,
                    "Station de depart la plus populaire le matin (de 7 h a 12 h inclus)",
                    "SELECT start_station, COUNT(*) AS rental_count " +
                            "FROM bike_rentals_view " +
                            "WHERE HOUR(start_time) BETWEEN 7 AND 12 " +
                            "GROUP BY start_station " +
                            "ORDER BY rental_count DESC, start_station " +
                            "LIMIT 1", DISPLAY_ROWS);

            printSection("6. Analyse du comportement des utilisateurs");
            showQuery(spark,
                    "Age moyen des utilisateurs",
                    "SELECT ROUND(AVG(age), 2) AS average_age " +
                            "FROM bike_rentals_view", DISPLAY_ROWS);

            showQuery(spark,
                    "Nombre de locations par genre",
                    "SELECT gender, COUNT(*) AS rental_count " +
                            "FROM bike_rentals_view " +
                            "GROUP BY gender " +
                            "ORDER BY rental_count DESC, gender", DISPLAY_ROWS);

            showQuery(spark,
                    "Nombre de locations par tranche d'age",
                    "SELECT " +
                            "  CASE " +
                            "    WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                            "    WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                            "    WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                            "    WHEN age >= 51 THEN '51+' " +
                            "    ELSE 'Hors plage' " +
                            "  END AS age_group, " +
                            "  COUNT(*) AS rental_count " +
                            "FROM bike_rentals_view " +
                            "GROUP BY " +
                            "  CASE " +
                            "    WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                            "    WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                            "    WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                            "    WHEN age >= 51 THEN '51+' " +
                            "    ELSE 'Hors plage' " +
                            "  END " +
                            "ORDER BY rental_count DESC, age_group", DISPLAY_ROWS);

        } finally {
            spark.stop();
        }
    }

    /**
     * Charge le CSV, elimine eventuellement le BOM UTF-8 du premier nom de colonne
     * et convertit les colonnes numeriques et temporelles dans les types utiles.
     */
    private static Dataset<Row> loadAndPrepareData(SparkSession spark, String inputPath) {
        Dataset<Row> rawData = spark.read()
                .option("header", "true")
                .option("encoding", "UTF-8")
                .option("mode", "FAILFAST")
                .csv(inputPath);

        // Le fichier fourni peut commencer par un BOM UTF-8 avant rental_id.
        // La plupart des lecteurs le suppriment, mais ce traitement couvre les deux cas.
        if (java.util.Arrays.asList(rawData.columns()).contains("\uFEFFrental_id")) {
            rawData = rawData.withColumnRenamed("\uFEFFrental_id", "rental_id");
        }

        return rawData
                .withColumn("rental_id", col("rental_id").cast("int"))
                .withColumn("age", col("age").cast("int"))
                .withColumn("duration_minutes", col("duration_minutes").cast("int"))
                .withColumn("price", col("price").cast("double"))
                .withColumn("start_time", to_timestamp(col("start_time"), TIMESTAMP_FORMAT))
                .withColumn("end_time", to_timestamp(col("end_time"), TIMESTAMP_FORMAT));
    }

    private static void showQuery(SparkSession spark, String title, String sql, int rowCount) {
        System.out.println();
        System.out.println("--- " + title + " ---");
        spark.sql(sql).show(rowCount, false);
    }

    private static void printSection(String title) {
        System.out.println();
        System.out.println("============================================================");
        System.out.println(title);
        System.out.println("============================================================");
    }
}
