package ru.pokrasko.dkvs.messages;

public class PingMessage extends Message {
    @Override
    public String toString() {
        return "ping";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PingMessage;
    }
}
