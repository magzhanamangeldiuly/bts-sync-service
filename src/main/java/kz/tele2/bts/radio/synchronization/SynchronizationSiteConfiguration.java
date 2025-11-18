package kz.tele2.bts.radio.synchronization;

import kz.tele2.bts.radio.db.sql.Atoll;
import kz.tele2.bts.radio.db.sql.Ciptracker;
import kz.tele2.bts.radio.db.sql.RanSharing;
import kz.tele2.bts.radio.handler.SitesInsertHandler;
import kz.tele2.bts.radio.model.Site;
import kz.tele2.bts.radio.transformers.AtollSitePropertiesTransformer;
import kz.tele2.bts.radio.transformers.NonExsistInPostgisTransformer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;

@Slf4j
@Configuration
public class SynchronizationSiteConfiguration {

    @Bean
    public JdbcPollingChannelAdapter syncSitesFlowAdapter(
            @Qualifier("postgresDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, RanSharing.SITES);
        adapter.setRowMapper(new BeanPropertyRowMapper<>(Site.class));
        adapter.setMaxRows(0);
        return adapter;
    }

    @Bean
    public JdbcPollingChannelAdapter syncRolloutSitesFlowAdapter(
            @Qualifier("ciptrackerDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, Ciptracker.ROLLOUT_SITES);
        adapter.setRowMapper(new BeanPropertyRowMapper<>(Site.class));
        adapter.setMaxRows(0);
        return adapter;
    }

    @Bean
    public JdbcPollingChannelAdapter syncTransportSitesFlowAdapter(
            @Qualifier("atollTrDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, Atoll.ATOLL_TR_SITES);
        adapter.setRowMapper(new BeanPropertyRowMapper<>(Site.class));
        adapter.setMaxRows(0);
        return adapter;
    }
    @Bean
    public JdbcPollingChannelAdapter syncSiteWorksFlowAdapter(
            @Qualifier("atollTrDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, Atoll.ATOLL_TR_SITES);
        adapter.setRowMapper(new BeanPropertyRowMapper<>(Site.class));
        adapter.setMaxRows(0);
        return adapter;
    }

    @Bean
    public IntegrationFlow syncSitesFlow(
            @Value("${sync.interval:21600000}") Long syncInterval,
            @Qualifier("syncSitesFlowAdapter") JdbcPollingChannelAdapter syncSitesFlowAdapter,
            SitesInsertHandler sitesInsertHandler,
            AtollSitePropertiesTransformer transformer
    ) {
        return IntegrationFlow
                .from(syncSitesFlowAdapter, e -> e.poller(p -> p.fixedDelay(syncInterval, 30_000)))
                .enrichHeaders(h -> h.header("insert_date", System.currentTimeMillis()))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(transformer)
                .handle(sitesInsertHandler)
                .get();
    }

    @Bean
    public IntegrationFlow syncRolloutSitesFlow(
            @Value("${sync.interval:21600000}") Long syncInterval,
            @Qualifier("syncRolloutSitesFlowAdapter") JdbcPollingChannelAdapter syncRolloutSitesFlowAdapter,
            SitesInsertHandler sitesInsertHandler,
            NonExsistInPostgisTransformer nonExsistInPostgisTransformer,
            AtollSitePropertiesTransformer atollSitePropertiesTransformer) {
        return IntegrationFlow
                .from(syncRolloutSitesFlowAdapter, e -> e.poller(p -> p.fixedDelay(syncInterval, 60_000)))
                .enrichHeaders(h -> h.header("insert_date", System.currentTimeMillis()))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(nonExsistInPostgisTransformer)
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(atollSitePropertiesTransformer)
                .handle(sitesInsertHandler)
                .get();
    }

    @Bean
    public IntegrationFlow syncTransportSitesFlow(
            @Value("${sync.interval:21600000}") Long syncInterval,
            @Qualifier("syncTransportSitesFlowAdapter") JdbcPollingChannelAdapter syncRolloutSitesFlowAdapter,
            SitesInsertHandler sitesInsertHandler,
            NonExsistInPostgisTransformer nonExsistInPostgisTransformer,
            AtollSitePropertiesTransformer atollSitePropertiesTransformer) {
        return IntegrationFlow
                .from(syncRolloutSitesFlowAdapter, e -> e.poller(p -> p.fixedDelay(syncInterval,  60_000)))
                .enrichHeaders(h -> h.header("insert_date", System.currentTimeMillis()))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(nonExsistInPostgisTransformer)
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(atollSitePropertiesTransformer)
                .handle(sitesInsertHandler)
                .get();
    }


    @Bean
    public IntegrationFlow syncSiteWorksFlow(
            @Value("${sync.interval:21600000}") Long syncInterval,
            @Qualifier("syncTransportSitesFlowAdapter") JdbcPollingChannelAdapter syncRolloutSitesFlowAdapter,
            SitesInsertHandler sitesInsertHandler,
            NonExsistInPostgisTransformer nonExsistInPostgisTransformer,
            AtollSitePropertiesTransformer atollSitePropertiesTransformer) {
        return IntegrationFlow
                .from(syncRolloutSitesFlowAdapter, e -> e.poller(p -> p.fixedDelay(syncInterval)))
                .enrichHeaders(h -> h.header("insert_date", System.currentTimeMillis()))
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(nonExsistInPostgisTransformer)
                .split()
                .aggregate(a -> a.sendPartialResultOnExpiry(true)
                        .expireGroupsUponCompletion(true)
                        .releaseStrategy(group -> group.size() >= 1000)
                        .groupTimeout(5000))
                .transform(atollSitePropertiesTransformer)
                .handle(sitesInsertHandler)
                .get();
    }
}