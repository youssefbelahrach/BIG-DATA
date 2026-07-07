package ma.enset.consumer;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * API REST de consultation du nombre de clics.
 *
 * Endpoint demandé par le TP :
 *   GET /clicks/count
 */
@RestController
public class ClickCountController {

    private final ClickCountListener listener;

    public ClickCountController(ClickCountListener listener) {
        this.listener = listener;
    }

    @GetMapping("/clicks/count")
    public Map<String, Object> getClickCount() {
        Map<String, Long> counts = listener.getCounts();

        // Cas 1 : l'application Streams publie une seule clé "total"
        if (counts.size() == 1 && counts.containsKey("total")) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("totalClicks", counts.get("total"));
            return response;
        }

        // Cas 2 : une clé par utilisateur (triée pour la lisibilité)
        return new LinkedHashMap<>(new TreeMap<>(counts));
    }

    /** Endpoint: total des clics */
    @GetMapping("/clicks/total")
    public Map<String, Object> getTotal() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("totalClicks", listener.getTotal());
        return response;
    }
}
