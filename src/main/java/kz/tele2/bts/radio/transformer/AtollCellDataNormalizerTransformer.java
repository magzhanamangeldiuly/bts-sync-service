package kz.tele2.bts.radio.transformer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kz.tele2.bts.radio.model.AtollCellData;
import kz.tele2.bts.radio.util.DataMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AtollCellDataNormalizerTransformer extends AbstractTransformer {

    @Override
    @SuppressWarnings("unchecked")
    protected Object doTransform(Message<?> message) {
        List<Map<String, Object>> atollCellData = (List<Map<String, Object>>) message.getHeaders().get("atollCellData");
        if (atollCellData == null || atollCellData.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, AtollCellData> result = new HashMap<>();
        for (Map<String, Object> rawCell : atollCellData) {
            AtollCellData cellData = DataMapper.toAtollCellData(rawCell);
            String key = cellData.toNormalizedKey();
            result.put(key, cellData);
        }
        return result;
    }
}

