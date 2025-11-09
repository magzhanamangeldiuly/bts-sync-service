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
        @Qualifier("sitesAtollDataAdapter") JdbcPollingChannelAdapter sitesAtollDataAdapter,
        AtollSitesNormalizerTransformer atollSitesNormalizerTransformer,
        SitesMergeTransformer sitesMergeTransformer,
        SitesUpsertHandler sitesUpsertHandler) {

        return IntegrationFlow.from("syncSitesChannel")
            .scatterGather(
                scatterer -> scatterer
                    .applySequence(true)
                    .recipientFlow(f -> f.handle(sitesRadioDataAdapter))
                    .recipientFlow(f -> f.handle(sitesMocnAdapter))
                    .recipientFlow(f -> f.handle(sites250PlusAdapter))
                    .recipientFlow(f -> f.handle(sitesAtollDataAdapter).transform(atollSitesNormalizerTransformer)),
                gather -> gather.outputProcessor(g -> {
                    List<Map<String, Object>> allSites = new ArrayList<>();
                    Map<String, Map<String, Object>> atollSitesMap = null;
                    
                    for (var msg : g.getMessages()) {
                        Object payload = msg.getPayload();
                        if (payload instanceof Map<?, ?>) {
                            @SuppressWarnings("unchecked")
                            Map<String, Map<String, Object>> temp = (Map<String, Map<String, Object>>) payload;
                            atollSitesMap = temp;
                        } else if (payload instanceof Collection<?>) {
                            for (Object obj : (Collection<?>) payload) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> site = (Map<String, Object>) obj;
                                allSites.add(site);
                            }
                        }
                    }
                    
                    return MessageBuilder.withPayload(allSites)
                        .setHeader("atollSitesMap", atollSitesMap)
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
        SitesUpsertHandler sitesUpsertHandler) {

        return IntegrationFlow.from("syncTransportSitesChannel")
            .enrichHeaders(h -> h.headerExpression("mainSites", "payload"))
            .handle(transportSitesAdapter)
            .transform(Message.class, this::filterNonMainSites)
            .handle(sitesUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractSiteNames)
            .get();
    }

    @Bean
    public IntegrationFlow syncRolloutSitesFlow(
        @Qualifier("rolloutSitesAdapter") JdbcPollingChannelAdapter rolloutSitesAdapter,
        SitesUpsertHandler sitesUpsertHandler) {

        return IntegrationFlow.from("syncRolloutSitesChannel")
            .enrichHeaders(h -> h.headerExpression("mainSites", "payload"))
            .handle(rolloutSitesAdapter)
            .transform(Message.class, this::filterNonMainSites)
            .handle(sitesUpsertHandler, e -> e.requiresReply(true))
            .transform(this::extractSiteNames)
            .get();
    }

    @Bean
    public IntegrationFlow syncCellsFlow(
        @Qualifier("cellsAdapter") JdbcPollingChannelAdapter cellsAdapter,
        @Qualifier("azimuthHeightAdapter") JdbcPollingChannelAdapter azimuthHeightAdapter,
        AtollDataNormalizerTransformer atollDataNormalizerTransformer,
        CellsEnrichmentTransformer cellsEnrichmentTransformer,
        CellsUpsertHandler cellsUpsertHandler) {

        return IntegrationFlow.from("syncCellsChannel")
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

        return IntegrationFlow.from("syncSiteWorksChannel")
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

        return IntegrationFlow.from("syncCellInterferenceChannel")
            .enrichHeaders(h -> h.headerExpression("syncedCells", "payload"))
            .handle(cellInterferenceAdapter)
            .transform(cellInterferenceFilterTransformer)
            .handle(cellInterferenceUpsertHandler, e -> e.requiresReply(true))
            .get();
    }

    @Bean
    public IntegrationFlow mainSyncFlow() {
        return IntegrationFlow
            .from(() -> MessageBuilder.withPayload(new Object()).build(), e -> e.poller(Pollers.fixedDelay(3_600_000L)))
            .gateway("syncSitesChannel")
            .enrichHeaders(h -> h.header("mainSyncedSites", "payload"))
            .gateway("syncTransportSitesChannel")
            .enrichHeaders(h -> h.header("transportSyncedSites", "payload"))
            .gateway("syncRolloutSitesChannel")
            .enrichHeaders(h -> h.header("rolloutSyncedSites", "payload"))
            .transform(Message.class, this::combineAllSites)
            .enrichHeaders(h -> h.header("allSyncedSites", "payload"))
            .gateway("syncCellsChannel")
            .enrichHeaders(h -> h.header("syncedCells", "payload"))
            .gateway("syncSiteWorksChannel")
            .gateway("syncCellInterferenceChannel")
            .get();
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

    @SuppressWarnings("unchecked")
    private Set<String> combineAllSites(Message<?> message) {
        Set<String> mainSites = (Set<String>) message.getHeaders().get("mainSyncedSites");
        Set<String> transportSites = (Set<String>) message.getHeaders().get("transportSyncedSites");
        Set<String> rolloutSites = (Set<String>) message.getHeaders().get("rolloutSyncedSites");

        Set<String> allSites = new HashSet<>();
        if (mainSites != null) {
            allSites.addAll(mainSites);
        }
        if (transportSites != null) {
            allSites.addAll(transportSites);
        }
        if (rolloutSites != null) {
            allSites.addAll(rolloutSites);
        }

        return allSites;
    }
}
