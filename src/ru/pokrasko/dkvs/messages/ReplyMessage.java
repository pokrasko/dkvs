package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.service.Result;

public class ReplyMessage extends ViewedMessage {
    private int requestNumber;
    private Result<?> result;

    public ReplyMessage(int viewNumber, int requestNumber, Result<?> result) {
        super(viewNumber);
        this.requestNumber = requestNumber;
        this.result = result;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public Result<?> getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "Reply " + viewNumber + " " + requestNumber + ": " + result;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ReplyMessage
                && requestNumber == ((ReplyMessage) o).requestNumber;
    }
}
