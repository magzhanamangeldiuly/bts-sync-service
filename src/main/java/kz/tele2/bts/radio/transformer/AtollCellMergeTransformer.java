package kz.tele2.bts.radio.transformer;

import kz.tele2.bts.radio.model.AtollCellData;
import kz.tele2.bts.radio.util.DataMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class AtollCellMergeTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        List<Map<String, Object>> rawCells = (List<Map<String, Object>>) message.getPayload();
        Map<String, AtollCellData> atollDataMap = 
            (Map<String, AtollCellData>) message.getHeaders().get("atollCellDataMap");
        return rawCells.stream()
            .map(DataMapper::toCellData)
            .map(cell -> {
                String key = cell.toAtollKey();
                AtollCellData atollData = atollDataMap.get(key);
                return cell.enrichWithAtoll(atollData);
            })
            .toList();
    }
}