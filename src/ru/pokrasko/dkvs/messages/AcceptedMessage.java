package ru.pokrasko.dkvs.messages;

import java.util.Collections;
import java.util.List;

public class AcceptedMessage extends Message {
    private int id;

    public AcceptedMessage() {
        super("ACCEPTED");
    }

    public AcceptedMessage(int id) {
        super("ACCEPTED");
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return _toString();
    }

    public static List<Token> tokens() {
        return Collections.emptyList();
    }

    public static AcceptedMessage construct(Object... data) {
        return construct(AcceptedMessage.class, new Class[] {}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof AcceptedMessage
                && id == ((AcceptedMessage) o).id;
    }
}
