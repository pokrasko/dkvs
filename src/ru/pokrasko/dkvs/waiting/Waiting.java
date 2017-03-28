package ru.pokrasko.dkvs.waiting;

import ru.pokrasko.dkvs.messages.Message;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Waiting {
    private boolean isBroadcast;
    private long expireTime;
    private long period;

    Waiting(boolean isBroadcast, long timeout, long period) {
        this.isBroadcast = isBroadcast;
        this.expireTime = System.currentTimeMillis() + timeout;
        this.period = period;
    }

    public List<Map.Entry<Integer, Message>> check(int receiver) {
        long time = System.currentTimeMillis();
        if (time >= expireTime) {
            expireTime = time + period;
            return getMessagesWithReceiver(receiver);
        } else {
            return null;
        }
    }

    protected List<Map.Entry<Integer, Message>> getMessagesWithReceiver(int receiver) {
        return getMessages().stream()
                .map(message -> new AbstractMap.SimpleEntry<>(isBroadcast ? -1 : receiver, message))
                .collect(Collectors.toList());
    }

    abstract List<Message> getMessages();
    public abstract void addMessage(Message message);
}
