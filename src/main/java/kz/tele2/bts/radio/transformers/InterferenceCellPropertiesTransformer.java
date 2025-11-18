package kz.tele2.bts.radio.transformers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kz.tele2.bts.radio.db.sql.Postgis;
import kz.tele2.bts.radio.model.Cell;
import kz.tele2.bts.radio.model.RanSharingCell;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static kz.tele2.bts.radio.utils.Utils.convertToMapByProperty;

@Component
public class InterferenceCellPropertiesTransformer extends AbstractTransformer {
    private final NamedParameterJdbcTemplate postgisNamedParameterJdbcTemplate;
    private final ObjectMapper objectMapper;

    public InterferenceCellPropertiesTransformer(@Qualifier("postgisNamedParameterJdbcTemplate") NamedParameterJdbcTemplate postgisNamedParameterJdbcTemplate,
                                                 ObjectMapper objectMapper) {
        this.postgisNamedParameterJdbcTemplate = postgisNamedParameterJdbcTemplate;
        this.objectMapper = objectMapper;
    }

    protected Object doTransform(Message<?> message) {
        var source = (List<Cell>) message.getPayload();
        var cells = source.stream().map(Cell::cell).collect(Collectors.toSet());
        var parameters = new MapSqlParameterSource();
        parameters.addValue("region", Postgis.DATASTORE_INTERFERENCE_REGION);
        parameters.addValue("keys", cells);
        var jsons =  postgisNamedParameterJdbcTemplate.queryForList(Postgis.SELECT_DATASTORE_QUERY, parameters, String.class);
        var result = jsons.stream().map(item -> {
            try {
                return (Map<String, Object>) objectMapper.readValue(item, Map.class);
            } catch (JsonProcessingException e) {
                return Map.of();
            }
        }).toList();

        var dataStoreMap = convertToMapByProperty(result, "cell");
        return source.stream().map(cell -> cell.appendInterference(dataStoreMap.get(cell.cell()))).toList();
    }
}
