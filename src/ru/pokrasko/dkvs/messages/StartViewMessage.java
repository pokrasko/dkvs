package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Log;

import java.util.Arrays;
import java.util.List;

public class StartViewMessage extends LogMessage implements ProtocolMessage {
    public StartViewMessage(Integer viewNumber, Log log, Integer opNumber, Integer commitNumber) {
        super("StartView", viewNumber, log, opNumber, commitNumber);
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.VIEW_CHANGE;
    }

    @Override
    public String toString() {
        return _toString(viewNumber, log, opNumber, commitNumber);
    }

    public static List<Token> tokens() {
        return Arrays.asList(new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.OBJECT, Log.class),
                new Token(Token.Type.INTEGER, null),
                new Token(Token.Type.INTEGER, null));
    }

    public static StartViewMessage construct(Object... data) {
        return construct(StartViewMessage.class, new Class[] {Integer.class, Log.class, Integer.class, Integer.class},
                data);
    }
}
