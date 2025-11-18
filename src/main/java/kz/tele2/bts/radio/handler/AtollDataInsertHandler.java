package kz.tele2.bts.radio.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import static kz.tele2.bts.radio.db.sql.Postgis.*;

@Slf4j
@Component
public class AtollDataInsertHandler {

    private final JdbcTemplate postgisJdbcTemplate;
    private final ObjectMapper objectMapper;

    public AtollDataInsertHandler(
            @Qualifier("postgisJdbcTemplate") JdbcTemplate postgisJdbcTemplate,
            ObjectMapper objectMapper) {
        this.postgisJdbcTemplate = postgisJdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void insertSite(Message<?> message) {
        insert(message, DATASTORE_SITE_REGION, "name");
    }

    public void insertCell(Message<?> message) {
        insert(message, DATASTORE_CELL_REGION, "key");
    }

    public void insertInterference(Message<?> message) {
        insert(message, DATASTORE_INTERFERENCE_REGION, "cell");
    }

    private void insert(Message<?> message, String region, String key) {
        var list = (List<Map<String, Object>>) message.getPayload();
        postgisJdbcTemplate.batchUpdate(INSERT_DATA_STORE,
                new BatchPreparedStatementSetter() {
                    @SneakyThrows
                    @Override
                    public void setValues(PreparedStatement ps, int i) {
                        var entry = list.get(i);
                        ps.setObject(1, entry.get(key));
                        ps.setObject(2, region);
                        ps.setObject(3, objectMapper.writeValueAsString(entry));
                    }

                    @Override
                    public int getBatchSize() {
                        return list.size();
                    }
                });
    }
}

