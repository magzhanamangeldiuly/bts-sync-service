package kz.tele2.bts.radio.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class CellInterferenceUpsertHandler extends AbstractReplyProducingMessageHandler {

    @Override
    @SuppressWarnings("unchecked")
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> interferences = (List<Map<String, Object>>) message.getPayload();
        
        log.info("Обрабатываем {} интерференций", interferences.size());
        
        for (Map<String, Object> interference : interferences) {
            upsertCellInterference(interference);
        }
        
        return interferences;
    }
    
    private void upsertCellInterference(Map<String, Object> interference) {
        
    }
}
