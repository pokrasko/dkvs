package ru.pokrasko.dkvs.messages;

import java.util.Collections;
import java.util.List;

public class PingMessage extends Message {
    public PingMessage() {
        super("ping");
    }

    @Override
    public String toString() {
        return _toString();
    }

    public static List<Token> tokens() {
        return Collections.emptyList();
    }

    public static PingMessage construct(Object... data) {
        return construct(PingMessage.class, new Class[] {}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PingMessage;
    }
}
