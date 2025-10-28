package kz.tele2.bts.radio.transformer;

 import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class AtollDataNormalizerTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        Collection<Map<String, Object>> rawAtollData = (Collection<Map<String, Object>>) message.getPayload();
        
        Map<String, Map<String, Object>> atollDataMap = new HashMap<>();
        
        for (Map<String, Object> rawData : rawAtollData) {
            String site = (String) rawData.get("site");
            String sector = (String) rawData.get("sector");
            
            if (site != null && sector != null) {
                String key = site.toUpperCase().trim() + "-" + sector.toUpperCase().trim();
                atollDataMap.put(key, rawData);
            }
        }
        
        log.debug("Нормализовали {} записей Atoll в {} уникальных ключей", rawAtollData.size(), atollDataMap.size());
        return atollDataMap;
    }
}