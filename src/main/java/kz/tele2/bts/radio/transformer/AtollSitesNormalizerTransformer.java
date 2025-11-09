package kz.tele2.bts.radio.transformer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
public class AtollSitesNormalizerTransformer extends AbstractTransformer {

    private static final Pattern FIVE_DIGITS = Pattern.compile("[0-9]{5}");
    private static final Pattern TWO_LETTERS_FOUR_DIGITS = Pattern.compile("[A-Z]{2}[0-9]{4}");

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        Collection<Map<String, Object>> rawAtollData = (Collection<Map<String, Object>>) message.getPayload();
        
        Map<String, Map<String, Object>> atollSitesMap = new HashMap<>();
        
        for (Map<String, Object> rawData : rawAtollData) {
            String name = (String) rawData.get("name");
            
            if (name != null) {
                String normalizedName = normalizeSiteName(name);
                
                if (atollSitesMap.containsKey(normalizedName)) {
                    Map<String, Object> existing = atollSitesMap.get(normalizedName);
                    mergeAtollData(existing, rawData);
                } else {
                    atollSitesMap.put(normalizedName, new HashMap<>(rawData));
                }
            }
        }
        
        log.debug("Нормализовали {} записей Atoll в {} уникальных сайтов", rawAtollData.size(), atollSitesMap.size());
        return atollSitesMap;
    }
    
    private String normalizeSiteName(String name) {
        String upperName = name.toUpperCase().trim();
        
        Matcher fiveDigitsMatcher = FIVE_DIGITS.matcher(upperName);
        if (fiveDigitsMatcher.find()) {
            return fiveDigitsMatcher.group();
        }
        
        Matcher twoLettersFourDigitsMatcher = TWO_LETTERS_FOUR_DIGITS.matcher(upperName);
        if (twoLettersFourDigitsMatcher.find()) {
            return twoLettersFourDigitsMatcher.group();
        }
        
        return upperName;
    }
    
    private void mergeAtollData(Map<String, Object> existing, Map<String, Object> incoming) {
        incoming.forEach((key, value) -> {
            if (value != null && !"name".equals(key)) {
                Object existingValue = existing.get(key);
                if (existingValue == null) {
                    existing.put(key, value);
                }
            }
        });
    }
}

