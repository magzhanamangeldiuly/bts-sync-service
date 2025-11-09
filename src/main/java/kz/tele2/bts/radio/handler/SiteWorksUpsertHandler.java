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
public class SiteWorksUpsertHandler extends AbstractReplyProducingMessageHandler {

    private final JdbcTemplate oracleJdbcTemplate;

    private static final String UPSERT_SQL = """
            INSERT INTO ob_works_v2 (site, work_type, status, insert_date)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """;

    public SiteWorksUpsertHandler(JdbcTemplate oracleJdbcTemplate) {
        this.oracleJdbcTemplate = oracleJdbcTemplate;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> works = (List<Map<String, Object>>) message.getPayload();
        
        log.info("Обрабатываем {} работ на сайтах", works.size());
        
        oracleJdbcTemplate.batchUpdate(UPSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> work = works.get(i);
                ps.setObject(1, work.get("site"));
                ps.setObject(2, work.get("work_type"));
                ps.setObject(3, work.get("status"));
            }

            @Override
            public int getBatchSize() {
                return works.size();
            }
        });
        
        return works;
    }
}
