package ru.pokrasko.dkvs.messages;

public abstract class ViewedMessage extends Message {
    int viewNumber;

    ViewedMessage(int viewNumber) {
        this.viewNumber = viewNumber;
    }

    public int getViewNumber() {
        return viewNumber;
    }
}
