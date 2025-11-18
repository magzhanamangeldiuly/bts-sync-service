package kz.tele2.bts.radio.handler;

import kz.tele2.bts.radio.model.Site;
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
public class SitesInsertHandler extends AbstractMessageHandler {

    private final JdbcTemplate postgisJdbcTemplate;

    public SitesInsertHandler(@Qualifier("postgisJdbcTemplate") JdbcTemplate postgisJdbcTemplate) {
        this.postgisJdbcTemplate = postgisJdbcTemplate;
    }

    @Override
    protected void handleMessageInternal(Message<?> message) {
        var list = (List<Site>) message.getPayload();
        var timestamp = message.getHeaders().get("insert_date", Long.class);
        postgisJdbcTemplate.batchUpdate("""
                insert into sites(site, rnc, bsc, latitude, longitude, operator, source, address, kato, insert_date)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                var entry = list.get(i);
                ps.setObject(1, entry.getName());
                ps.setObject(2, entry.getRnc());
                ps.setObject(3, entry.getBsc());
                ps.setObject(4, entry.getLatitude());
                ps.setObject(5, entry.getLatitude());
                ps.setObject(6, entry.getOperator());
                ps.setObject(7, entry.getSource());
                ps.setObject(8, entry.getAddress());
                ps.setObject(9, entry.getKato());
                ps.setObject(10, new Timestamp(timestamp));
            }

            @Override
            public int getBatchSize() {
                return list.size();
            }
        });
    }
}

