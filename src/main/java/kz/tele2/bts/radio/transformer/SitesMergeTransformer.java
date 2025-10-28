package kz.tele2.bts.radio.transformer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SitesMergeTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        Collection<Map<String, Object>> rawSites = (Collection<Map<String, Object>>) message.getPayload();
        
        Map<String, Map<String, Object>> siteMap = new HashMap<>();
        
        for (Map<String, Object> rawSite : rawSites) {
            String name = (String) rawSite.get("name");
            if (name != null) {
                String normalizedName = name.toUpperCase().trim();
                siteMap.merge(normalizedName, rawSite, this::mergeSites);
            }
        }
        
        List<Map<String, Object>> mergedSites = siteMap.values().stream()
                .peek(site -> site.put("name", site.get("name").toString().toUpperCase().trim()))
                .collect(Collectors.toList());
        
        log.debug("Объединили {} сайтов в {} уникальных", rawSites.size(), mergedSites.size());
        return mergedSites;
    }
    
    private Map<String, Object> mergeSites(Map<String, Object> existing, Map<String, Object> incoming) {
        Map<String, Object> merged = new HashMap<>(existing);
        
        incoming.forEach((key, value) -> {
            if (value != null && merged.get(key) == null) {
                merged.put(key, value);
            }
        });
        
        return merged;
    }
}
