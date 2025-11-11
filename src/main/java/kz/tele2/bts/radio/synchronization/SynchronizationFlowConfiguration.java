package kz.tele2.bts.radio.synchronization;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import kz.tele2.bts.radio.handler.CellInterferenceUpsertHandler;
import kz.tele2.bts.radio.handler.CellsUpsertHandler;
import kz.tele2.bts.radio.handler.RolloutSitesUpsertHandler;
import kz.tele2.bts.radio.handler.SiteWorksUpsertHandler;
import kz.tele2.bts.radio.handler.SitesUpsertHandler;
import kz.tele2.bts.radio.handler.TransportSitesUpsertHandler;
import kz.tele2.bts.radio.transformer.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@Slf4j
@Configuration
public class SynchronizationFlowConfiguration {

    @Bean
    public IntegrationFlow sitesRadioDataFlow(
        @Qualifier("sitesRadioDataAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("source", "RADIO_DATA").header("aggregationKey", "sites"))
            .channel("sitesAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow sitesMocnFlow(
        @Qualifier("sitesMocnAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("source", "KCELL_CM").header("aggregationKey", "sites"))
            .channel("sitesAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow sites250PlusFlow(
        @Qualifier("sites250PlusAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("source", "250_PLUS").header("aggregationKey", "sites"))
            .channel("sitesAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow sitesAtollDataFlow(
        @Qualifier("sitesAtollDataAdapter") JdbcPollingChannelAdapter adapter,
        AtollSitesNormalizerTransformer transformer) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .transform(transformer)
            .channel("atollSitesDataChannel")
            .get();
    }

    @Bean
    public IntegrationFlow sitesAggregationFlow(
        SitesMergeTransformer sitesMergeTransformer,
        SitesUpsertHandler sitesUpsertHandler) {

        return IntegrationFlow.from("sitesAggregationChannel")
            .aggregate(a -> a
                .correlationStrategy(m -> m.getHeaders().get("aggregationKey"))
                .releaseStrategy(g -> g.size() >= 500 || (System.currentTimeMillis() - g.getTimestamp()) > 2000)
                .groupTimeout(2000L)
                .sendPartialResultOnExpiry(true)
                .expireGroupsUponCompletion(true)
            )
            .transform(Message.class, this::aggregateMessages)
            .transform(sitesMergeTransformer)
            .handle(sitesUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractSiteNames)
            .channel("mainSitesResultChannel")
            .get();
    }

    @Bean
    public IntegrationFlow transportSitesFlow(
        @Qualifier("transportSitesAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("aggregationKey", "transportSites"))
            .channel("transportSitesAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow transportSitesAggregationFlow(TransportSitesUpsertHandler transportSitesUpsertHandler) {
        return IntegrationFlow.from("transportSitesAggregationChannel")
            .aggregate(a -> a
                .correlationStrategy(m -> m.getHeaders().get("aggregationKey"))
                .releaseStrategy(g -> g.size() >= 500 || (System.currentTimeMillis() - g.getTimestamp()) > 2000)
                .groupTimeout(2000L)
                .sendPartialResultOnExpiry(true)
                .expireGroupsUponCompletion(true)
            )
            .transform(Message.class, this::aggregateMessages)
            .filter(Message.class, this::filterNonMainSites)
            .handle(transportSitesUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractSiteNames)
            .channel("transportSitesResultChannel")
            .get();
    }

    @Bean
    public IntegrationFlow rolloutSitesFlow(
        @Qualifier("rolloutSitesAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("aggregationKey", "rolloutSites"))
            .channel("rolloutSitesAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow rolloutSitesAggregationFlow(RolloutSitesUpsertHandler rolloutSitesUpsertHandler) {
        return IntegrationFlow.from("rolloutSitesAggregationChannel")
            .aggregate(a -> a
                .correlationStrategy(m -> m.getHeaders().get("aggregationKey"))
                .releaseStrategy(g -> g.size() >= 500 || (System.currentTimeMillis() - g.getTimestamp()) > 2000)
                .groupTimeout(2000L)
                .sendPartialResultOnExpiry(true)
                .expireGroupsUponCompletion(true)
            )
            .transform(Message.class, this::aggregateMessages)
            .filter(Message.class, this::filterNonMainSites)
            .handle(rolloutSitesUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractSiteNames)
            .channel("rolloutSitesResultChannel")
            .get();
    }

    @Bean
    public IntegrationFlow azimuthHeightFlow(
        @Qualifier("azimuthHeightAdapter") JdbcPollingChannelAdapter adapter,
        AtollDataNormalizerTransformer transformer) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .transform(transformer)
            .channel("atollDataChannel")
            .get();
    }

    @Bean
    public IntegrationFlow cellsFlow(
        @Qualifier("cellsAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("aggregationKey", "cells"))
            .channel("cellsAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow cellsAggregationFlow(
        CellsEnrichmentTransformer cellsEnrichmentTransformer,
        CellsUpsertHandler cellsUpsertHandler) {

        return IntegrationFlow.from("cellsAggregationChannel")
            .aggregate(a -> a
                .correlationStrategy(m -> m.getHeaders().get("aggregationKey"))
                .releaseStrategy(g -> g.size() >= 1000 || (System.currentTimeMillis() - g.getTimestamp()) > 2000)
                .groupTimeout(2000L)
                .sendPartialResultOnExpiry(true)
                .expireGroupsUponCompletion(true)
            )
            .transform(Message.class, this::aggregateMessages)
            .transform(cellsEnrichmentTransformer)
            .handle(cellsUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractCellNames)
            .channel("cellsResultChannel")
            .get();
    }

    @Bean
    public IntegrationFlow siteWorksFlow(
        @Qualifier("siteWorksAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("aggregationKey", "works"))
            .channel("worksAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow worksAggregationFlow(
        SiteWorksFilterTransformer siteWorksFilterTransformer,
        SiteWorksUpsertHandler siteWorksUpsertHandler) {

        return IntegrationFlow.from("worksAggregationChannel")
            .aggregate(a -> a
                .correlationStrategy(m -> m.getHeaders().get("aggregationKey"))
                .releaseStrategy(g -> g.size() >= 500 || (System.currentTimeMillis() - g.getTimestamp()) > 2000)
                .groupTimeout(2000L)
                .sendPartialResultOnExpiry(true)
                .expireGroupsUponCompletion(true)
            )
            .transform(Message.class, this::aggregateMessages)
            .transform(siteWorksFilterTransformer)
            .handle(siteWorksUpsertHandler, e -> e.requiresReply(true))
            .channel("worksResultChannel")
            .get();
    }

    @Bean
    public IntegrationFlow cellInterferenceFlow(
        @Qualifier("cellInterferenceAdapter") JdbcPollingChannelAdapter adapter) {
        return IntegrationFlow.from(adapter, e -> e.poller(Pollers.fixedDelay(Long.MAX_VALUE)))
            .split()
            .enrichHeaders(h -> h.header("aggregationKey", "interferences"))
            .channel("interferencesAggregationChannel")
            .get();
    }

    @Bean
    public IntegrationFlow interferencesAggregationFlow(
        CellInterferenceFilterTransformer cellInterferenceFilterTransformer,
        CellInterferenceUpsertHandler cellInterferenceUpsertHandler) {

        return IntegrationFlow.from("interferencesAggregationChannel")
            .aggregate(a -> a
                .correlationStrategy(m -> m.getHeaders().get("aggregationKey"))
                .releaseStrategy(g -> g.size() >= 1000 || (System.currentTimeMillis() - g.getTimestamp()) > 2000)
                .groupTimeout(2000L)
                .sendPartialResultOnExpiry(true)
                .expireGroupsUponCompletion(true)
            )
            .transform(Message.class, this::aggregateMessages)
            .transform(cellInterferenceFilterTransformer)
            .handle(cellInterferenceUpsertHandler, e -> e.requiresReply(true))
            .channel("interferencesResultChannel")
            .get();
    }

    @Bean
    public IntegrationFlow mainSyncFlow() {
        return IntegrationFlow
            .from(() -> MessageBuilder.withPayload(new Object()).build(),
                e -> e.poller(Pollers.fixedDelay(3_600_000L)))
            .enrichHeaders(h -> h.header("insert_date", new Timestamp(System.currentTimeMillis())))
            .publishSubscribeChannel(s -> s
                .subscribe(f -> f.channel("sitesAggregationChannel"))
                .subscribe(f -> f.channel("transportSitesAggregationChannel"))
                .subscribe(f -> f.channel("rolloutSitesAggregationChannel"))
            )
            .bridge(e -> e.poller(Pollers.fixedDelay(100)))
            .channel("mainSitesResultChannel")
            .enrichHeaders(h -> h.header("allSyncedSites", "payload"))
            .publishSubscribeChannel(s -> s
                .subscribe(f -> f.channel("cellsAggregationChannel"))
                .subscribe(f -> f.channel("worksAggregationChannel"))
            )
            .bridge(e -> e.poller(Pollers.fixedDelay(100)))
            .channel("cellsResultChannel")
            .enrichHeaders(h -> h.header("syncedCells", "payload"))
            .channel("interferencesAggregationChannel")
            .get();
    }

    @SuppressWarnings("unchecked")
    private Message<?> aggregateMessages(Message<?> message) {
        Collection<Message<?>> messages = (Collection<Message<?>>) message.getPayload();

        List<Map<String, Object>> items = messages.stream()
            .map(msg -> (Map<String, Object>) msg.getPayload())
            .collect(Collectors.toList());

        return MessageBuilder.withPayload(items)
            .copyHeaders(message.getHeaders())
            .build();
    }

    private Set<String> extractSiteNames(List<Map<String, Object>> sites) {
        if (sites == null || sites.isEmpty()) {
            return Set.of();
        }
        return sites.stream()
            .filter(site -> site.get("name") != null)
            .map(site -> site.get("name").toString().toUpperCase().trim())
            .collect(Collectors.toSet());
    }

    private Set<String> extractCellNames(List<Map<String, Object>> cells) {
        if (cells == null || cells.isEmpty()) {
            return Set.of();
        }
        return cells.stream()
            .filter(cell -> cell.get("cell") != null)
            .map(cell -> cell.get("cell").toString().toUpperCase().trim())
            .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private boolean filterNonMainSites(Message<?> message) {
        List<Map<String, Object>> candidateSites = (List<Map<String, Object>>) message.getPayload();
        Set<String> mainSites = (Set<String>) message.getHeaders().get("mainSites");

        if (mainSites == null || mainSites.isEmpty()) {
            return false;
        }

        List<Map<String, Object>> filtered = candidateSites.stream()
            .filter(site -> {
                String name = (String) site.get("name");
                return name != null && !mainSites.contains(name.toUpperCase().trim());
            })
            .collect(Collectors.toList());

        return !filtered.isEmpty();
    }
}
