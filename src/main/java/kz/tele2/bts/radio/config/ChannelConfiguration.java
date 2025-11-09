package kz.tele2.bts.radio.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;

@Configuration
public class ChannelConfiguration {

    @Bean("sitesAggregationChannel")
    public QueueChannel sitesAggregationChannel() {
        return new QueueChannel(10000);
    }

    @Bean("atollSitesDataChannel")
    public QueueChannel atollSitesDataChannel() {
        return new QueueChannel(5000);
    }

    @Bean("mainSitesResultChannel")
    public QueueChannel mainSitesResultChannel() {
        return new QueueChannel(1000);
    }

    @Bean("transportSitesAggregationChannel")
    public QueueChannel transportSitesAggregationChannel() {
        return new QueueChannel(5000);
    }

    @Bean("transportSitesResultChannel")
    public QueueChannel transportSitesResultChannel() {
        return new QueueChannel(1000);
    }

    @Bean("rolloutSitesAggregationChannel")
    public QueueChannel rolloutSitesAggregationChannel() {
        return new QueueChannel(5000);
    }

    @Bean("rolloutSitesResultChannel")
    public QueueChannel rolloutSitesResultChannel() {
        return new QueueChannel(1000);
    }

    @Bean("cellsAggregationChannel")
    public QueueChannel cellsAggregationChannel() {
        return new QueueChannel(20000);
    }

    @Bean("atollDataChannel")
    public QueueChannel atollDataChannel() {
        return new QueueChannel(10000);
    }

    @Bean("cellsResultChannel")
    public QueueChannel cellsResultChannel() {
        return new QueueChannel(1000);
    }

    @Bean("worksAggregationChannel")
    public QueueChannel worksAggregationChannel() {
        return new QueueChannel(5000);
    }

    @Bean("worksResultChannel")
    public QueueChannel worksResultChannel() {
        return new QueueChannel(1000);
    }

    @Bean("interferencesAggregationChannel")
    public QueueChannel interferencesAggregationChannel() {
        return new QueueChannel(20000);
    }

    @Bean("interferencesResultChannel")
    public QueueChannel interferencesResultChannel() {
        return new QueueChannel(1000);
    }
}
