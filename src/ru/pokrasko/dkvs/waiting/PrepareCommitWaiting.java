package ru.pokrasko.dkvs.waiting;

import ru.pokrasko.dkvs.messages.CommitMessage;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PrepareMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PrepareCommitWaiting extends Waiting {
    private int viewNumber;
    private int opNumber;
    private List<Message> messages;

    public PrepareCommitWaiting(int viewNumber, int opNumber, long timeout, long period) {
        super(true, timeout, period);
        this.viewNumber = viewNumber;
        this.opNumber = opNumber;
        this.messages = new ArrayList<>();
    }

    @Override
    public void addMessage(Message message) {
//        System.err.println("Adding prepare message to broadcast");
        assert message instanceof PrepareMessage && ((PrepareMessage) message).getOpNumber() == opNumber + 1;
        messages.add(message);
        opNumber++;
//        System.err.println("Added successfully");
    }

    @Override
    List<Message> getMessages() {
        return !messages.isEmpty() ? messages : Collections.singletonList(new CommitMessage(viewNumber, opNumber));
    }

    public void updateCommitNumber(int commitNumber) {
        int remained = opNumber - commitNumber;
        messages = new ArrayList<>(messages.subList(messages.size() - remained, messages.size()));
    }
}
