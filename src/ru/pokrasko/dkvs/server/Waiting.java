package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.messages.Message;

class Waiting {
    private Message message;
    private int sender;
    private long expireTime;
    private long period;

    Waiting(Message message, int sender, long timeout, long period) {
        this.message = message;
        this.sender = sender;
        this.expireTime = System.currentTimeMillis() + timeout;
        this.period = period;
    }

    Waiting(Message message, long timeout, long period) {
        this(message, -1, timeout, period);
    }

    Message getMessage() {
        return message;
    }

    int getSender() {
        return sender;
    }

    boolean isTimedOut() {
        long time = System.currentTimeMillis();
        if (time >= expireTime) {
            expireTime = time + period;
            return true;
        } else {
            return false;
        }
    }
}
