package kz.tele2.bts.radio.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class CellInterferenceUpsertHandler extends AbstractReplyProducingMessageHandler {

    private final JdbcTemplate oracleJdbcTemplate;

    private static final String UPSERT_SQL = """
            MERGE INTO interferences_v2 t
            USING (SELECT ? AS cell, ? AS value FROM dual) s
            ON (t.cell = s.cell)
            WHEN MATCHED THEN
                UPDATE SET t.value = s.value, t.insert_date = SYSTIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (cell, value, insert_date)
                VALUES (s.cell, s.value, SYSTIMESTAMP)
            """;

    public CellInterferenceUpsertHandler(JdbcTemplate oracleJdbcTemplate) {
        this.oracleJdbcTemplate = oracleJdbcTemplate;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> interferences = (List<Map<String, Object>>) message.getPayload();
        
        log.info("Обрабатываем {} интерференций", interferences.size());
        
        oracleJdbcTemplate.batchUpdate(UPSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> interference = interferences.get(i);
                ps.setObject(1, interference.get("cell"));
                ps.setObject(2, interference.get("value"));
            }

            @Override
            public int getBatchSize() {
                return interferences.size();
            }
        });
        
        return interferences;
    }
}
