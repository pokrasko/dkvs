package ru.pokrasko.dkvs.messages;

import java.util.Collections;
import java.util.List;

public class NewReplicaMessage extends Message {
    private int id;

    public NewReplicaMessage(Integer id) {
        super("node");
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return _toString(id);
    }

    public static List<Token> tokens() {
        return Collections.singletonList(new Token(Token.Type.INTEGER, null));
    }

    public static NewReplicaMessage construct(Object... data) {
        return construct(NewReplicaMessage.class, new Class[] {Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof NewReplicaMessage
                && id == ((NewReplicaMessage) o).id;
    }
}
