package kz.tele2.bts.radio.synchronization;

import kz.tele2.bts.radio.db.sql.Atoll;
import kz.tele2.bts.radio.db.sql.MariaDb;
import kz.tele2.bts.radio.handler.AtollDataInsertHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import javax.sql.DataSource;

//@Configuration
public class SynchronizationDataStoreConfiguration {

    @Bean
    public JdbcPollingChannelAdapter atollSitesAdapter(
            @Qualifier("atollDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, Atoll.ATOLL_SITES);
        adapter.setRowMapper(new ColumnMapRowMapper());
        adapter.setMaxRows(0);
        return adapter;
    }

    @Bean
    public JdbcPollingChannelAdapter atollCellsAdapter(
            @Qualifier("atollDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, Atoll.ATOLL_CELLS);
        adapter.setRowMapper(new ColumnMapRowMapper());
        adapter.setMaxRows(0);
        return adapter;
    }

    @Bean
    public JdbcPollingChannelAdapter cellInterferenceAdapter(
            @Qualifier("mariaDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, MariaDb.CELL_INTERFERENCE);
        adapter.setRowMapper(new ColumnMapRowMapper());
        adapter.setMaxRows(0);
        return adapter;
    }

    @Bean
    public IntegrationFlow syncAtollSites(
            @Value("${sync.interval:21600000}") Long syncInterval,
            @Qualifier("atollSitesAdapter") JdbcPollingChannelAdapter atollSitesAdapter,
            AtollDataInsertHandler dataStoreUpsertHandler) {
        return IntegrationFlow
                .from(atollSitesAdapter, e -> e.poller(Pollers.fixedDelay(syncInterval)))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .handle(dataStoreUpsertHandler, "insertSite")
                .get();
    }

    @Bean
    public IntegrationFlow syncAtollCells(
            @Value("${sync.interval:21600000}") Long syncInterval,
            @Qualifier("atollCellsAdapter") JdbcPollingChannelAdapter atollCellsAdapter,
            AtollDataInsertHandler dataStoreUpsertHandler) {
        return IntegrationFlow
                .from(atollCellsAdapter, e -> e.poller(Pollers.fixedDelay(syncInterval)))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 2000)
                        .groupTimeout(5000))
                .handle(dataStoreUpsertHandler, "insertCell")
                .get();
    }

    @Bean
    public IntegrationFlow syncCellInterferences(
            @Value("${sync.interval:21600000}") Long syncInterval,
            @Qualifier("cellInterferenceAdapter") JdbcPollingChannelAdapter atollCellsAdapter,
            AtollDataInsertHandler dataStoreUpsertHandler) {
        return IntegrationFlow
                .from(atollCellsAdapter, e -> e.poller(Pollers.fixedDelay(syncInterval)))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 2000)
                        .groupTimeout(5000))
                .handle(dataStoreUpsertHandler, "insertInterference")
                .get();
    }

}
