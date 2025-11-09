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
public class SitesUpsertHandler extends AbstractReplyProducingMessageHandler {

    private final JdbcTemplate oracleJdbcTemplate;

    private static final String UPSERT_SQL = """
            MERGE INTO sites_v2 t
            USING (SELECT ? AS name, ? AS rnc, ? AS bsc, ? AS latitude, ? AS longitude, 
                          ? AS operator, ? AS kato, ? AS is_test, ? AS address, ? AS source, ? AS type 
                   FROM dual) s
            ON (t.name = s.name)
            WHEN MATCHED THEN
                UPDATE SET t.rnc = s.rnc, t.bsc = s.bsc, t.latitude = s.latitude, 
                           t.longitude = s.longitude, t.operator = s.operator, t.kato = s.kato,
                           t.is_test = s.is_test, t.address = s.address, t.source = s.source, 
                           t.type = s.type, t.insert_date = SYSTIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (name, rnc, bsc, latitude, longitude, operator, kato, is_test, address, source, type, insert_date)
                VALUES (s.name, s.rnc, s.bsc, s.latitude, s.longitude, s.operator, s.kato, s.is_test, 
                        s.address, s.source, s.type, SYSTIMESTAMP)
            """;

    public SitesUpsertHandler(JdbcTemplate oracleJdbcTemplate) {
        this.oracleJdbcTemplate = oracleJdbcTemplate;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> sites = (List<Map<String, Object>>) message.getPayload();
        
        log.info("Обрабатываем {} сайтов", sites.size());
        
        oracleJdbcTemplate.batchUpdate(UPSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> site = sites.get(i);
                ps.setObject(1, site.get("name"));
                ps.setObject(2, site.get("rnc"));
                ps.setObject(3, site.get("bsc"));
                ps.setObject(4, site.get("latitude"));
                ps.setObject(5, site.get("longitude"));
                ps.setObject(6, site.get("operator"));
                ps.setObject(7, site.get("kato"));
                ps.setObject(8, site.get("is_test"));
                ps.setObject(9, site.get("address"));
                ps.setObject(10, site.get("source"));
                ps.setObject(11, site.get("type"));
            }

            @Override
            public int getBatchSize() {
                return sites.size();
            }
        });
        
        return sites;
    }
}
