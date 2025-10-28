package kz.tele2.bts.radio.transformer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CellsEnrichmentTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        Collection<Map<String, Object>> cells = (Collection<Map<String, Object>>) message.getPayload();
        Map<String, Map<String, Object>> atollDataMap = (Map<String, Map<String, Object>>) message.getHeaders().get("atollDataMap");
        Set<String> syncedSites = (Set<String>) message.getHeaders().get("allSyncedSites");
        
        List<Map<String, Object>> enrichedCells = cells.stream()
                .filter(cell -> {
                    String site = (String) cell.get("site");
                    return site != null && syncedSites != null && syncedSites.contains(site.toUpperCase().trim());
                })
                .map(cell -> enrichCellWithAtoll(cell, atollDataMap))
                .collect(Collectors.toList());
        
        log.debug("Обогатили {} ячеек для {} синхронизированных сайтов", enrichedCells.size(), syncedSites != null ? syncedSites.size() : 0);
        return enrichedCells;
    }
    
    private Map<String, Object> enrichCellWithAtoll(Map<String, Object> cell, Map<String, Map<String, Object>> atollDataMap) {
        Map<String, Object> enrichedCell = new HashMap<>(cell);
        
        String site = (String) cell.get("site");
        String sector = (String) cell.get("sector");
        
        if (site != null && sector != null) {
            String key = site.toUpperCase().trim() + "-" + sector.toUpperCase().trim();
            Map<String, Object> atollData = atollDataMap.get(key);
            
            if (atollData != null) {
                Object azimut = atollData.get("azimut");
                Object height = atollData.get("height");
                
                if (azimut != null) enrichedCell.put("azimut", azimut);
                if (height != null) enrichedCell.put("height", height);
            }
        }
        
        return enrichedCell;
    }
}
