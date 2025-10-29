package kz.tele2.bts.radio.handler;

import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class CellsDeleteHandler extends AbstractMessageHandler {

    @Override
    protected void handleMessageInternal(Message<?> message) {
        // TODO Delete
    }
}