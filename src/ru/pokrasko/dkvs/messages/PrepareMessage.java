package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Request;

import java.util.Arrays;
import java.util.List;

public class PrepareMessage extends ViewNumberMessage {
    private Request<?, ?> request;
    private int opNumber;
    private int commitNumber;

    public PrepareMessage(Integer viewNumber, Request<?, ?> request, Integer opNumber, Integer commitNumber) {
        super("Prepare", viewNumber);
        this.request = request;
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }

    public Request<?, ?> getRequest() {
        return request;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    @Override
    public String toString() {
        return _toString(viewNumber, request, opNumber, commitNumber);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.OBJECT, Request.class),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static PrepareMessage construct(Object... data) {
        return construct(PrepareMessage.class, new Class[] {Integer.class, Request.class, Integer.class, Integer.class},
                data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrepareMessage
                && opNumber == ((PrepareMessage) o).opNumber
                && commitNumber == ((PrepareMessage) o).commitNumber;
    }
}
