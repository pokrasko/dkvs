package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Log;

import java.util.Arrays;
import java.util.List;

public class DoViewChangeMessage extends LogMessage implements ProtocolMessage {
    public static final int VIEW_CHANGE_LOG_SUFFIX_SIZE = 3;

    private int lastNormalViewNumber;
    private int replicaId;

    public DoViewChangeMessage(Integer viewNumber, Log log, Integer lastNormalViewNumber,
                               Integer opNumber, Integer commitNumber, Integer replicaId) {
        super("DoViewChange", viewNumber, log, opNumber, commitNumber);
        this.lastNormalViewNumber = lastNormalViewNumber;
        this.replicaId = replicaId;
    }

    public int getLastNormalViewNumber() {
        return lastNormalViewNumber;
    }

    public int getReplicaId() {
        return replicaId;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.VIEW_CHANGE;
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.OBJECT, Log.class),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static DoViewChangeMessage construct(Object... data) {
        return construct(DoViewChangeMessage.class,
                new Class[] {Integer.class, Log.class, Integer.class, Integer.class, Integer.class, Integer.class},
                data);
    }

    @Override
    public String toString() {
        return _toString(viewNumber, log, lastNormalViewNumber, opNumber, commitNumber, replicaId);
    }
}
