package kz.tele2.bts.radio.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class CellsUpsertHandler extends AbstractReplyProducingMessageHandler {

    @Override
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> cells = (List<Map<String, Object>>) message.getPayload();
        
        log.info("Обрабатываем {} ячеек", cells.size());
        
        for (Map<String, Object> cell : cells) {
            upsertCell(cell);
        }
        
        return cells;
    }
    
    private void upsertCell(Map<String, Object> cell) {
        log.info("Cells: {}", cell.toString());
    }
}