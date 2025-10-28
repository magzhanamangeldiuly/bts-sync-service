package kz.tele2.bts.radio.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SitesUpsertHandler extends AbstractReplyProducingMessageHandler {

    @Override
    @SuppressWarnings("unchecked")
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> sites = (List<Map<String, Object>>) message.getPayload();
        
        log.info("Обрабатываем {} сайтов", sites.size());
        
        for (Map<String, Object> site : sites) {
            upsertSite(site);
        }
        
        return sites;
    }
    
    private void upsertSite(Map<String, Object> site) {
        
    }
}