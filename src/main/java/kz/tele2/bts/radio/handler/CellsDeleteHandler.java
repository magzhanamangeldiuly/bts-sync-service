package kz.tele2.bts.radio.handler;

import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import static kz.tele2.bts.radio.sql.Oracle.DELETE_CELLS;

@Component
public class CellsDeleteHandler extends AbstractMessageHandler {
    private final JdbcTemplate oracleJdbcTemplate;

    public CellsDeleteHandler(JdbcTemplate oracleJdbcTemplate) {
        this.oracleJdbcTemplate = oracleJdbcTemplate;
    }

    @Override
    protected void handleMessageInternal(Message<?> message) {
        oracleJdbcTemplate.execute(DELETE_CELLS);
    }
}