package kz.tele2.bts.radio.transformers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kz.tele2.bts.radio.db.sql.Postgis;
import kz.tele2.bts.radio.model.Site;
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
public class SiteNameTransformer extends AbstractTransformer {
    private final NamedParameterJdbcTemplate postgisNamedParameterJdbcTemplate;
    private final ObjectMapper objectMapper;

    public SiteNameTransformer(@Qualifier("postgisNamedParameterJdbcTemplate") NamedParameterJdbcTemplate postgisNamedParameterJdbcTemplate,
                               ObjectMapper objectMapper) {
        this.postgisNamedParameterJdbcTemplate = postgisNamedParameterJdbcTemplate;
        this.objectMapper = objectMapper;
    }

    protected Object doTransform(Message<?> message) {
        var sourceSites = (List<Site>) message.getPayload();
        return sourceSites.stream().map(Site::getName).collect(Collectors.toSet());
    }
}
