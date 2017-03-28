package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Log;

import java.util.Arrays;
import java.util.List;

public class RecoveryResponseMessage extends LogMessage {
    private long nonce;
    private int replicaId;

    public RecoveryResponseMessage(Integer viewNumber, Long nonce, Log log, Integer opNumber, Integer commitNumber,
                                   Integer replicaId) {
        super("RecoveryResponse", viewNumber, log, opNumber, commitNumber);
        this.nonce = nonce;
        this.replicaId = replicaId;
    }

    public long getNonce() {
        return nonce;
    }

    public int getReplicaId() {
        return replicaId;
    }

    @Override
    public String toString() {
        return _toString(viewNumber, nonce, log, opNumber, commitNumber, replicaId);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.LONG, null),
                new Token(Token.Type.OBJECT, Log.class),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static RecoveryResponseMessage construct(Object... data) {
        return construct(RecoveryResponseMessage.class,
                new Class[] {Integer.class, Long.class, Log.class, Integer.class, Integer.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }
}
