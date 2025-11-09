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
public class CellsUpsertHandler extends AbstractReplyProducingMessageHandler {

    private final JdbcTemplate oracleJdbcTemplate;

    private static final String UPSERT_SQL = """
            MERGE INTO ob_cells_v2 t
            USING (SELECT ? AS cell, ? AS site, ? AS sector, ? AS cellid, ? AS lac, 
                          ? AS type, ? AS status, ? AS band, ? AS azimut, ? AS height, ? AS insert_date 
                   FROM dual) s
            ON (t.cell = s.cell)
            WHEN MATCHED THEN
                UPDATE SET t.site = s.site, t.sector = s.sector, t.cellid = s.cellid, 
                           t.lac = s.lac, t.type = s.type, t.status = s.status, 
                           t.band = s.band, t.azimut = s.azimut, t.height = s.height,
                           t.insert_date = s.insert_date
            WHEN NOT MATCHED THEN
                INSERT (cell, site, sector, cellid, lac, type, status, band, azimut, height, insert_date)
                VALUES (s.cell, s.site, s.sector, s.cellid, s.lac, s.type, s.status, 
                        s.band, s.azimut, s.height, s.insert_date)
            """;

    public CellsUpsertHandler(JdbcTemplate oracleJdbcTemplate) {
        this.oracleJdbcTemplate = oracleJdbcTemplate;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object handleRequestMessage(Message<?> message) {
        List<Map<String, Object>> cells = (List<Map<String, Object>>) message.getPayload();
        java.sql.Timestamp insertDate = (java.sql.Timestamp) message.getHeaders().get("insert_date");
        
        log.info("Обрабатываем {} ячеек", cells.size());
        
        oracleJdbcTemplate.batchUpdate(UPSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> cell = cells.get(i);
                ps.setObject(1, cell.get("cell"));
                ps.setObject(2, cell.get("site"));
                ps.setObject(3, cell.get("sector"));
                ps.setObject(4, cell.get("cellid"));
                ps.setObject(5, cell.get("lac"));
                ps.setObject(6, cell.get("type"));
                ps.setObject(7, cell.get("status"));
                ps.setObject(8, cell.get("band"));
                ps.setObject(9, cell.get("azimut"));
                ps.setObject(10, cell.get("height"));
                ps.setTimestamp(11, insertDate);
            }

            @Override
            public int getBatchSize() {
                return cells.size();
            }
        });
        
        return cells;
    }
}
