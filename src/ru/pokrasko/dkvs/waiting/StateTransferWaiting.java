package ru.pokrasko.dkvs.waiting;

import ru.pokrasko.dkvs.messages.Message;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StateTransferWaiting extends SimpleWaiting {
    private int receiver;

    public StateTransferWaiting(Message message, int receiver, long timeout, long period) {
        super(message, receiver == -1, timeout, period);
        this.receiver = receiver;
    }

    @Override
    public List<Map.Entry<Integer, Message>> check(int receiver) {
        return super.check(this.receiver);
    }

    @Override
    protected List<Map.Entry<Integer, Message>> getMessagesWithReceiver(int receiver) {
        return Collections.singletonList(new AbstractMap.SimpleEntry<>(receiver, message));
    }
}
