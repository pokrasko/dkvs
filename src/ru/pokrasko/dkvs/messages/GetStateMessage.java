package ru.pokrasko.dkvs.messages;

import java.util.Arrays;
import java.util.List;

public class GetStateMessage extends ViewNumberMessage {
    private int opNumber;
    private int replicaId;

    public GetStateMessage(Integer viewNumber, Integer opNumber, Integer replicaId) {
        super("GetState", viewNumber);
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
    public String toString() {
        return _toString(viewNumber, opNumber, replicaId);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static GetStateMessage construct(Object... data) {
        return construct(GetStateMessage.class, new Class[] {Integer.class, Integer.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof GetStateMessage
                && viewNumber == ((GetStateMessage) o).viewNumber
                && opNumber == ((GetStateMessage) o).opNumber
                && replicaId == ((GetStateMessage) o).replicaId;
    }
}
