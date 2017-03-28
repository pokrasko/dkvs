package ru.pokrasko.dkvs.messages;

public abstract class ViewNumberMessage extends Message {
    int viewNumber;

    ViewNumberMessage(String type, int viewNumber) {
        super(type);
        this.viewNumber = viewNumber;
    }

    public int getViewNumber() {
        return viewNumber;
    }
}
