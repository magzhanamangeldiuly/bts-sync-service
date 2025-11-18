package kz.tele2.bts.radio.synchronization;

import kz.tele2.bts.radio.db.sql.RanSharing;
import kz.tele2.bts.radio.handler.CellsInsertHandler;
import kz.tele2.bts.radio.model.RanSharingCell;
import kz.tele2.bts.radio.transformers.AtollCellPropertiesTransformer;
import kz.tele2.bts.radio.transformers.InterferenceCellPropertiesTransformer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.jdbc.core.DataClassRowMapper;

import javax.sql.DataSource;

@Slf4j
//@Configuration
public class SynchronizationCellsConfiguration {

    @Bean
    public JdbcPollingChannelAdapter syncCellsFlowAdapter(
            @Qualifier("postgresDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, RanSharing.CELLS);
        adapter.setRowMapper(new DataClassRowMapper<>(RanSharingCell.class));
        adapter.setMaxRows(0);
        return adapter;
    }


    @Bean
    public IntegrationFlow syncCellsFlow(
            @Qualifier("syncCellsFlowAdapter") JdbcPollingChannelAdapter syncCellsFlowAdapter,
            CellsInsertHandler cellsInsertHandler,
            AtollCellPropertiesTransformer atollCellPropertiesTransformer,
            InterferenceCellPropertiesTransformer interferenceCellPropertiesTransformer
    ) {
        return IntegrationFlow
                .from(syncCellsFlowAdapter, e -> e.poller(p -> p.fixedDelay(1_000_000, 0)))
                .enrichHeaders(h -> h.header("insert_date", System.currentTimeMillis()))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(atollCellPropertiesTransformer)
                .transform(interferenceCellPropertiesTransformer)
                .handle(cellsInsertHandler)
                .get();
    }
}
