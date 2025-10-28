package kz.tele2.bts.radio.synchronization;

import java.util.*;
import java.util.stream.Collectors;
import kz.tele2.bts.radio.handler.CellInterferenceUpsertHandler;
import kz.tele2.bts.radio.handler.CellsUpsertHandler;
import kz.tele2.bts.radio.handler.SiteWorksUpsertHandler;
import kz.tele2.bts.radio.handler.SitesUpsertHandler;
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
    public IntegrationFlow syncSitesFlow(
        @Qualifier("sitesRadioDataAdapter") JdbcPollingChannelAdapter sitesRadioDataAdapter,
        @Qualifier("sitesMocnAdapter") JdbcPollingChannelAdapter sitesMocnAdapter,
        @Qualifier("sites250PlusAdapter") JdbcPollingChannelAdapter sites250PlusAdapter,
        SitesMergeTransformer sitesMergeTransformer,
        SitesUpsertHandler sitesUpsertHandler) {
        return IntegrationFlow.from(sitesRadioDataAdapter, c -> c.poller(Pollers.fixedDelay(3600000L)))
            .scatterGather(
                scatterer -> scatterer
                    .applySequence(true)
                    .recipientFlow(f -> f.handle(sitesRadioDataAdapter))
                    .recipientFlow(f -> f.handle(sitesMocnAdapter))
                    .recipientFlow(f -> f.handle(sites250PlusAdapter)),
                gather -> gather.outputProcessor(g -> {
                    List<Map<String, Object>> mainSites = g.streamMessages()
                        .flatMap(m -> ((Collection<?>) m.getPayload()).stream())
                        .map(obj -> (Map<String, Object>) obj)
                        .collect(Collectors.toList());
                    return MessageBuilder.withPayload(mainSites)
                        .copyHeaders(g.getOne().getHeaders())
                        .build();
                })
            )
            .transform(sitesMergeTransformer)
            .handle(sitesUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractSiteNames)
            .get();
    }

    @Bean
    public IntegrationFlow syncTransportSitesFlow(
        @Qualifier("transportSitesAdapter") JdbcPollingChannelAdapter transportSitesAdapter,
        SitesMergeTransformer sitesMergeTransformer,
        SitesUpsertHandler sitesUpsertHandler) {

        return IntegrationFlow.from(transportSitesAdapter, c -> c.poller(Pollers.fixedDelay(3600000L)))
            .enrichHeaders(h -> h.headerExpression("mainSites", "payload"))
            .handle(transportSitesAdapter)
            .transform(Message.class, this::filterNonMainSites)
            .transform(sitesMergeTransformer)
            .handle(sitesUpsertHandler, e -> e.requiresReply(true))
            .get();
    }

    @Bean
    public IntegrationFlow syncRolloutSitesFlow(
        @Qualifier("rolloutSitesAdapter") JdbcPollingChannelAdapter rolloutSitesAdapter,
        SitesMergeTransformer sitesMergeTransformer,
        SitesUpsertHandler sitesUpsertHandler) {

        return IntegrationFlow.from(rolloutSitesAdapter, c -> c.poller(Pollers.fixedDelay(3600000L)))
            .enrichHeaders(h -> h.headerExpression("mainSites", "payload"))
            .handle(rolloutSitesAdapter)
            .transform(Message.class, this::filterNonMainSites)
            .transform(sitesMergeTransformer)
            .handle(sitesUpsertHandler, e -> e.requiresReply(true))
            .get();
    }

    @Bean
    public IntegrationFlow syncCellsFlow(
        @Qualifier("cellsAdapter") JdbcPollingChannelAdapter cellsAdapter,
        @Qualifier("azimuthHeightAdapter") JdbcPollingChannelAdapter azimuthHeightAdapter,
        AtollDataNormalizerTransformer atollDataNormalizerTransformer,
        CellsEnrichmentTransformer cellsEnrichmentTransformer,
        CellsUpsertHandler cellsUpsertHandler) {

        return IntegrationFlow.from(cellsAdapter, c -> c.poller(Pollers.fixedDelay(3600000L)))
            .enrichHeaders(h -> h.headerExpression("allSyncedSites", "payload"))
            .handle(azimuthHeightAdapter)
            .transform(atollDataNormalizerTransformer)
            .enrichHeaders(h -> h.headerExpression("atollDataMap", "payload"))
            .handle(cellsAdapter)
            .transform(cellsEnrichmentTransformer)
            .handle(cellsUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractCellNames)
            .get();
    }

    @Bean
    public IntegrationFlow syncSiteWorksFlow(
        @Qualifier("siteWorksAdapter") JdbcPollingChannelAdapter siteWorksAdapter,
        SiteWorksFilterTransformer siteWorksFilterTransformer,
        SiteWorksUpsertHandler siteWorksUpsertHandler) {

        return IntegrationFlow.from(siteWorksAdapter, c -> c.poller(Pollers.fixedDelay(3600000L)))
            .enrichHeaders(h -> h.headerExpression("allSyncedSites", "payload"))
            .handle(siteWorksAdapter)
            .transform(siteWorksFilterTransformer)
            .handle(siteWorksUpsertHandler, e -> e.requiresReply(true))
            .get();
    }

    @Bean
    public IntegrationFlow syncCellInterferenceFlow(
        @Qualifier("cellInterferenceAdapter") JdbcPollingChannelAdapter cellInterferenceAdapter,
        CellInterferenceFilterTransformer cellInterferenceFilterTransformer,
        CellInterferenceUpsertHandler cellInterferenceUpsertHandler) {
        return IntegrationFlow.from(cellInterferenceAdapter, c -> c.poller(Pollers.fixedDelay(3600000L)))
            .enrichHeaders(h -> h.headerExpression("syncedCells", "payload"))
            .handle(cellInterferenceAdapter)
            .transform(cellInterferenceFilterTransformer)
            .handle(cellInterferenceUpsertHandler, e -> e.requiresReply(true))
            .get();
    }

    @SuppressWarnings("unchecked")
    private Set<String> extractSiteNames(List<Map<String, Object>> sites) {
        if (sites == null || sites.isEmpty()) {
            return Set.of();
        }
        return sites.stream()
            .filter(site -> site.get("name") != null)
            .map(site -> site.get("name").toString().toUpperCase().trim())
            .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
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
    private List<Map<String, Object>> filterNonMainSites(Message<?> message) {
        Collection<Map<String, Object>> candidateSites = (Collection<Map<String, Object>>) message.getPayload();
        Set<String> mainSites = (Set<String>) message.getHeaders().get("mainSites");

        if (mainSites == null || mainSites.isEmpty()) {
            return List.of();
        }

        return candidateSites.stream()
            .filter(site -> {
                String name = (String) site.get("name");
                return name != null && !mainSites.contains(name.toUpperCase().trim());
            })
            .collect(Collectors.toList());
    }

    @Bean
    public IntegrationFlow mainSyncFlow(
        IntegrationFlow syncSitesFlow,
        IntegrationFlow syncTransportSitesFlow,
        IntegrationFlow syncRolloutSitesFlow,
        IntegrationFlow syncCellsFlow,
        IntegrationFlow syncSiteWorksFlow,
        IntegrationFlow syncCellInterferenceFlow) {
        return IntegrationFlow.from("syncTrigger", false)
            .publishSubscribeChannel(pubsub -> pubsub
                .subscribe(syncSitesFlow)
                .subscribe(syncTransportSitesFlow)
                .subscribe(syncRolloutSitesFlow)
            )
            .transform(this::combineAllSites)
            .publishSubscribeChannel(pubsub -> pubsub
                .subscribe(syncCellsFlow)
                .subscribe(syncSiteWorksFlow)
            )
            .handle(syncCellInterferenceFlow)
            .get();
    }

    @SuppressWarnings("unchecked")
    private Set<String> combineAllSites(Message<?> message) {
        Set<String> mainSites = (Set<String>) message.getHeaders().get("syncedSites");
        Set<String> transportSites = (Set<String>) message.getHeaders().get("syncedSites");
        Set<String> rolloutSites = (Set<String>) message.getHeaders().get("syncedSites");
        Set<String> allSites = new HashSet<>();
        if (mainSites != null) allSites.addAll(mainSites);
        if (transportSites != null) allSites.addAll(transportSites);
        if (rolloutSites != null) allSites.addAll(rolloutSites);
        return allSites;
    }
}
