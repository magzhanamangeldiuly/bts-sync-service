package kz.tele2.bts.radio.synchronization;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import kz.tele2.bts.radio.db.sql.Queries;
import kz.tele2.bts.radio.handler.SitesUpsertHandler;
import kz.tele2.bts.radio.model.Site;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.messaging.Message;

@Slf4j
@Configuration
public class SynchronizationFlowConfiguration {

    private final BeanPropertyRowMapper<Site> rowMapper = new BeanPropertyRowMapper<>(Site.class);

    @Bean
    public JdbcPollingChannelAdapter sitesAdapter(
        @Qualifier("postgresDataSource") DataSource dataSource) {
        JdbcPollingChannelAdapter adapter = new JdbcPollingChannelAdapter(dataSource, Queries.SITES);
        adapter.setRowMapper(rowMapper);
        return adapter;
    }

    @Bean
    public IntegrationFlow sitesSyncScheduledFlow(
        @Qualifier("sitesAdapter") JdbcPollingChannelAdapter sitesAdapter,
        SitesUpsertHandler sitesUpsertHandler
    ) {
        return IntegrationFlow
            .from(sitesAdapter,e -> e.poller(Pollers.fixedDelay(1000)))
            .enrichHeaders(h -> h.header("insert_date", new Timestamp(System.currentTimeMillis())))
            .split()
            .aggregate(a -> a
                .correlationStrategy(m -> "batch")
                .releaseStrategy(g -> g.size() >= 500)
                .groupTimeout(5000)
                .sendPartialResultOnExpiry(true)
            )
            .transform(Message.class, this::collectBatch)
            .transform(Site.class)
            .handle(sitesUpsertHandler, "handle")
            .get();
    }

    @SuppressWarnings("unchecked")
    private List<Site> collectBatch(Message<?> message) {
        Collection<Message<?>> messages = (Collection<Message<?>>) message.getPayload();
        return messages.stream()
            .map(msg -> (Site) msg.getPayload())
            .toList();
    }
}
