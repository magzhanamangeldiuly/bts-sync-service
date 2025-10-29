package kz.tele2.bts.radio.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SiteWorksUpsertHandler extends AbstractReplyProducingMessageHandler {

    @Override
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> works = (List<Map<String, Object>>) message.getPayload();
        
        log.info("Обрабатываем {} работ на сайтах", works.size());
        
        for (Map<String, Object> work : works) {
            upsertSiteWork(work);
        }
        
        return works;
    }
    
    private void upsertSiteWork(Map<String, Object> work) {
        log.info("SiteWork: {}", work.toString());
    }
}
