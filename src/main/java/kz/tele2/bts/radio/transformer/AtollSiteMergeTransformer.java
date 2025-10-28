package kz.tele2.bts.radio.transformer;

import kz.tele2.bts.radio.model.SiteData;
import kz.tele2.bts.radio.util.DataMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class AtollSiteMergeTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        List<Map<String, Object>> rawSites = (List<Map<String, Object>>) message.getPayload();
        Map<String, SiteData> atollDataMap = (Map<String, SiteData>) message.getHeaders().get("atollDataMap");
        return rawSites.stream()
            .map(DataMapper::toSiteData)
            .map(site -> {
                String siteName = site.name();
                if (siteName == null) {
                    return site;
                }
                SiteData atollData = atollDataMap.get(siteName);
                return site.mergeWith(atollData);
            })
            .toList();
    }
}
