package ru.pokrasko.dkvs.messages;

public class ReplyMessage<R> extends ViewedMessage {
    private int requestNumber;
    private R result;

    public ReplyMessage(int viewNumber, int requestNumber, R result) {
        super(viewNumber);
        this.requestNumber = requestNumber;
        this.result = result;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public R getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "Reply " + viewNumber + " " + requestNumber + " " + result;
    }
}
