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
public class SiteWorksFilterTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        Collection<Map<String, Object>> works = (Collection<Map<String, Object>>) message.getPayload();
        Set<String> syncedSites = (Set<String>) message.getHeaders().get("allSyncedSites");
        
        List<Map<String, Object>> filteredWorks = works.stream()
                .filter(work -> {
                    String site = (String) work.get("site");
                    return site != null && syncedSites != null && syncedSites.contains(site.toUpperCase().trim());
                })
                .collect(Collectors.toList());
        
        log.debug("Отфильтровали {} работ для {} синхронизированных сайтов", filteredWorks.size(), syncedSites != null ? syncedSites.size() : 0);
        return filteredWorks;
    }
}
