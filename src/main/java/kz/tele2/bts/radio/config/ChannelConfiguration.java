package kz.tele2.bts.radio.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;

@Configuration
public class ChannelConfiguration {

    @Bean("syncTrigger")
    public DirectChannel syncTriggerChannel() {
        return new DirectChannel();
    }
}
