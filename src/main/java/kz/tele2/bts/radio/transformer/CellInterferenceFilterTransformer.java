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
public class CellInterferenceFilterTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        Collection<Map<String, Object>> interferences = (Collection<Map<String, Object>>) message.getPayload();
        Set<String> syncedCells = (Set<String>) message.getHeaders().get("syncedCells");
        
        List<Map<String, Object>> filteredInterferences = interferences.stream()
                .filter(interference -> {
                    String cell = (String) interference.get("cell");
                    return cell != null && syncedCells != null && syncedCells.contains(cell.toUpperCase().trim());
                })
                .collect(Collectors.toList());
        
        log.debug("Отфильтровали {} интерференций для {} синхронизированных ячеек", filteredInterferences.size(), syncedCells != null ? syncedCells.size() : 0);
        return filteredInterferences;
    }
}
