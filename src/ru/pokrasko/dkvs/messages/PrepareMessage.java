package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Request;

public class PrepareMessage extends ViewedMessage {
    private Request<?, ?> request;
    private int opNumber;
    private int commitNumber;

    public PrepareMessage(int viewNumber, int opNumber, int commitNumber, Request<?, ?> request) {
        super(viewNumber);
        this.request = request;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }

    public Request<?, ?> getRequest() {
        return request;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    @Override
    public String toString() {
        return "Prepare " + viewNumber + " " + opNumber + " " + commitNumber + " " + request;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrepareMessage
                && opNumber == ((PrepareMessage) o).opNumber
                && commitNumber == ((PrepareMessage) o).commitNumber;
    }
}
