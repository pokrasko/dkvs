package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.service.Result;

import java.util.Arrays;
import java.util.List;

public class ReplyMessage extends ViewNumberMessage {
    private int requestNumber;
    private Result<?> result;

    public ReplyMessage(Integer viewNumber, Integer requestNumber, Result<?> result) {
        super("Reply", viewNumber);
        this.requestNumber = requestNumber;
        this.result = result;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public Result<?> getResult() {
        return result;
    }

    @Override
    public String toString() {
        return _toString(viewNumber, requestNumber, "<" + result + ">");
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.OBJECT, Result.class));
    }

    public static ReplyMessage construct(Object... data) {
        return construct(ReplyMessage.class, new Class[] {Integer.class, Integer.class, Result.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ReplyMessage
                && requestNumber == ((ReplyMessage) o).requestNumber;
    }
}
