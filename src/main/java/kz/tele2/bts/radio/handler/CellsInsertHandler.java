package kz.tele2.bts.radio.handler;

import kz.tele2.bts.radio.model.Cell;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;


@Component
public class CellsInsertHandler extends AbstractMessageHandler {

    private final JdbcTemplate postgisJdbcTemplate;

    public CellsInsertHandler(@Qualifier("postgisJdbcTemplate") JdbcTemplate postgisJdbcTemplate) {
        this.postgisJdbcTemplate = postgisJdbcTemplate;
    }

    @Override
    protected void handleMessageInternal(Message<?> message) {
        var list = (List<Cell>) message.getPayload();
        var timestamp = message.getHeaders().get("insert_date", Long.class);
        postgisJdbcTemplate.batchUpdate("""
                insert into cells(cell, site, sector, cellid, lac, type, status, band, azimuth, height, interference, insert_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                var entry = list.get(i);
                ps.setObject(1, entry.cell());
                ps.setObject(2, entry.site());
                ps.setObject(3, entry.sector());
                ps.setObject(4, entry.cellId());
                ps.setObject(5, entry.lac());
                ps.setObject(6, entry.type());
                ps.setObject(7, entry.status());
                ps.setObject(8, entry.band());
                ps.setObject(9, entry.azimuth());
                ps.setObject(10, entry.height());
                ps.setObject(11, entry.interference());
                ps.setObject(12, new Timestamp(timestamp));
            }

            @Override
            public int getBatchSize() {
                return list.size();
            }
        });
    }
}

