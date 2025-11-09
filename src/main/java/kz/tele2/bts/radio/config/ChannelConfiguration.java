package kz.tele2.bts.radio.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;

@Configuration
public class ChannelConfiguration {

    @Bean("syncSitesChannel")
    public DirectChannel syncSitesChannel() {
        return new DirectChannel();
    }

    @Bean("syncTransportSitesChannel")
    public DirectChannel syncTransportSitesChannel() {
        return new DirectChannel();
    }

    @Bean("syncRolloutSitesChannel")
    public DirectChannel syncRolloutSitesChannel() {
        return new DirectChannel();
    }

    @Bean("syncCellsChannel")
    public DirectChannel syncCellsChannel() {
        return new DirectChannel();
    }

    @Bean("syncSiteWorksChannel")
    public DirectChannel syncSiteWorksChannel() {
        return new DirectChannel();
    }

    @Bean("syncCellInterferenceChannel")
    public DirectChannel syncCellInterferenceChannel() {
        return new DirectChannel();
    }
}
