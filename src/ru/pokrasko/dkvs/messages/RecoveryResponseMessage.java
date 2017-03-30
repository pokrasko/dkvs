package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Log;

import java.util.Arrays;
import java.util.List;

public class RecoveryResponseMessage extends LogMessage implements ProtocolMessage {
    private long nonce;
    private int replicaId;

    public RecoveryResponseMessage(Integer viewNumber, Long nonce, Integer replicaId,
                                   Log log, Integer opNumber, Integer commitNumber) {
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
    public Protocol getProtocol() {
        return Protocol.RECOVERY;
    }

    @Override
    public String toString() {
        return log != null
                ? _toString(viewNumber, nonce, replicaId, log, opNumber, commitNumber)
                : _toString(viewNumber, nonce, replicaId);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.LONG, null),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.OBJECT, Log.class),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static RecoveryResponseMessage construct(Object... data) {
        return construct(RecoveryResponseMessage.class,
                new Class[] {Integer.class, Long.class, Integer.class, Log.class, Integer.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }
}
