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
        Map<String, Map<String, Object>> atollSitesMap = (Map<String, Map<String, Object>>) message.getHeaders().get("atollSitesMap");
        
        Map<String, Map<String, Object>> siteMap = new HashMap<>();
        
        for (Map<String, Object> rawSite : rawSites) {
            String name = (String) rawSite.get("name");
            if (name != null) {
                String normalizedName = name.toUpperCase().trim();
                siteMap.merge(normalizedName, rawSite, this::mergeSites);
            }
        }
        
        if (atollSitesMap != null) {
            atollSitesMap.forEach((atollSiteName, atollSiteProperties) -> {
                Map<String, Object> site = siteMap.get(atollSiteName);
                if (site != null) {
                    atollSiteProperties.forEach((key, value) -> {
                        if (value != null && !value.toString().isEmpty()) {
                            site.putIfAbsent(key, value);
                        }
                    });
                }
            });
        }
        
        List<Map<String, Object>> mergedSites = siteMap.values().stream()
                .peek(site -> site.put("name", site.get("name").toString().toUpperCase().trim()))
                .collect(Collectors.toList());
        
        log.debug("Объединили {} сайтов в {} уникальных", rawSites.size(), mergedSites.size());
        return mergedSites;
    }
    
    private Map<String, Object> mergeSites(Map<String, Object> existing, Map<String, Object> incoming) {
        Map<String, Object> merged = new HashMap<>(existing);
        
        String[] priorityFields = {"latitude", "longitude", "kato", "address"};
        String existingSource = (String) existing.get("source");
        String incomingSource = (String) incoming.get("source");
        
        for (String field : priorityFields) {
            Object existingValue = existing.get(field);
            Object incomingValue = incoming.get(field);
            
            if (incomingValue != null && !incomingValue.toString().isEmpty()) {
                if (existingValue == null || existingValue.toString().isEmpty()) {
                    merged.put(field, incomingValue);
                } else if (shouldPreferIncoming(existingSource, incomingSource)) {
                    merged.put(field, incomingValue);
                }
            }
        }
        
        incoming.forEach((key, value) -> {
            if (value != null && !value.toString().isEmpty() && !merged.containsKey(key)) {
                merged.put(key, value);
            }
        });
        
        return merged;
    }
    
    private boolean shouldPreferIncoming(String existingSource, String incomingSource) {
        if (incomingSource == null) return false;
        if (existingSource == null) return true;
        
        int existingPriority = getSourcePriority(existingSource);
        int incomingPriority = getSourcePriority(incomingSource);
        
        return incomingPriority > existingPriority;
    }
    
    private int getSourcePriority(String source) {
        return switch (source.toUpperCase()) {
            case "ATOLL" -> 4;
            case "250_PLUS" -> 3;
            case "KCELL_CM" -> 2;
            case "RADIO_DATA" -> 1;
            default -> 0;
        };
    }
}
