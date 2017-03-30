package ru.pokrasko.dkvs.messages;


import java.util.Arrays;
import java.util.List;

public class StartViewChangeMessage extends ViewNumberMessage implements ProtocolMessage {
    private int replicaId;

    public StartViewChangeMessage(Integer viewNumber, Integer replicaId) {
        super("StartViewChange", viewNumber);
        this.replicaId = replicaId;
    }

    public int getReplicaId() {
        return replicaId;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.VIEW_CHANGE;
    }

    @Override
    public String toString() {
        return _toString(viewNumber, replicaId);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static StartViewChangeMessage construct(Object... data) {
        return construct(StartViewChangeMessage.class, new Class[] {Integer.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof StartViewChangeMessage
                && viewNumber == ((StartViewChangeMessage) o).viewNumber
                && replicaId == ((StartViewChangeMessage) o).replicaId;
    }
}
