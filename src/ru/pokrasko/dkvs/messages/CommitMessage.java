package ru.pokrasko.dkvs.messages;

import java.util.Arrays;
import java.util.List;

public class CommitMessage extends CommitNumberMessage implements ProtocolMessage {
    public CommitMessage(Integer viewNumber, Integer commitNumber) {
        super("Commit", viewNumber, commitNumber);
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.NORMAL;
    }

    @Override
    public String toString() {
        return _toString(viewNumber, commitNumber);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static CommitMessage construct(Object... data) {
        return construct(CommitMessage.class, new Class[] {Integer.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CommitMessage
                && commitNumber == ((CommitMessage) o).commitNumber;
    }
}
