package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Log;

import java.util.Arrays;
import java.util.List;

public class NewStateMessage extends LogMessage implements ProtocolMessage {
    public NewStateMessage(Integer viewNumber, Log log, Integer opNumber, Integer commitNumber) {
        super("NewState", viewNumber, log, opNumber, commitNumber);
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.STATE_TRANSFER;
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

    public static NewStateMessage construct(Object... data) {
        return construct(NewStateMessage.class,
                new Class[] {Integer.class, Log.class, Integer.class, Integer.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof NewStateMessage
                && viewNumber == ((NewStateMessage) o).viewNumber
                && opNumber == ((NewStateMessage) o).opNumber
                && commitNumber == ((NewStateMessage) o).commitNumber;
    }
}
