package ru.pokrasko.dkvs.messages;

import java.util.Arrays;
import java.util.List;

public class RecoveryMessage extends Message {
    private int replicaId;
    private long nonce;
    private int opNumber;

    public RecoveryMessage(int replicaId, int opNumber) {
        super("Recovery");
        this.replicaId = replicaId;
        this.nonce = System.currentTimeMillis();
        this.opNumber = opNumber;
    }

    public RecoveryMessage(Integer replicaId, Long nonce, Integer opNumber) {
        super("Recovery");
        this.replicaId = replicaId;
        this.nonce = nonce;
        this.opNumber = opNumber;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public long getNonce() {
        return nonce;
    }

    public int getOpNumber() {
        return opNumber;
    }

    @Override
    public String toString() {
        return _toString(replicaId, nonce, opNumber);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.LONG, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static RecoveryMessage construct(Object... data) {
        return construct(RecoveryMessage.class, new Class[] {Integer.class, Long.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RecoveryMessage
                && replicaId == ((RecoveryMessage) o).replicaId
                && nonce == ((RecoveryMessage) o).nonce;
    }
}
