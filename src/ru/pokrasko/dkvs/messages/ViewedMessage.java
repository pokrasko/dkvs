package ru.pokrasko.dkvs.messages;

public abstract class ViewedMessage extends Message {
    protected int viewNumber;

    protected ViewedMessage(int viewNumber) {
        this.viewNumber = viewNumber;
    }

    public int getViewNumber() {
        return viewNumber;
    }
}
