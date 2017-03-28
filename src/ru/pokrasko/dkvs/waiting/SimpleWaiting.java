package ru.pokrasko.dkvs.waiting;

import ru.pokrasko.dkvs.messages.Message;

import java.util.Collections;
import java.util.List;

public class SimpleWaiting extends Waiting {
    protected Message message;

    public SimpleWaiting(Message message, boolean isBroadcast, long timeout, long period) {
        super(isBroadcast, timeout, period);
        this.message = message;
    }

    @Override
    public void addMessage(Message message) {
        this.message = message;
    }

    @Override
    List<Message> getMessages() {
        return Collections.singletonList(message);
    }
}
