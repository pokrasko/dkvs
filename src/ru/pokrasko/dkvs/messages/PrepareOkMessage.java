package ru.pokrasko.dkvs.messages;


import java.util.Arrays;
import java.util.List;

public class PrepareOkMessage extends ViewNumberMessage implements ProtocolMessage {
    private int opNumber;
    private int replicaId;

    public PrepareOkMessage(Integer viewNumber, Integer opNumber, Integer replicaId) {
        super("PrepareOk", viewNumber);
        this.opNumber = opNumber;
        this.replicaId = replicaId;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getReplicaId() {
        return replicaId;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.NORMAL;
    }

    @Override
    public String toString() {
        return _toString(viewNumber, opNumber, replicaId);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static PrepareOkMessage construct(Object... data) {
        return construct(PrepareOkMessage.class, new Class[] {Integer.class, Integer.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrepareOkMessage
                && opNumber == ((PrepareOkMessage) o).opNumber
                && replicaId == ((PrepareOkMessage) o).replicaId;
    }
}
