package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Request;

import java.util.Arrays;
import java.util.List;

public class PrepareMessage extends CommitNumberMessage implements ProtocolMessage {
    private Request<?, ?> request;
    private int opNumber;

    public PrepareMessage(Integer viewNumber, Request<?, ?> request, Integer opNumber, Integer commitNumber) {
        super("Prepare", viewNumber, commitNumber);
        this.request = request;
        this.opNumber = opNumber;
    }

    public Request<?, ?> getRequest() {
        return request;
    }

    public int getOpNumber() {
        return opNumber;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.NORMAL;
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
