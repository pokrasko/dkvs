package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Request;

import java.util.Collections;
import java.util.List;

public class RequestMessage extends Message implements ProtocolMessage {
    private Request<?, ?> request;

    public RequestMessage(Request<?, ?> request) {
        super("Request");
        this.request = request;
    }

    public Request<?, ?> getRequest() {
        return request;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.NORMAL;
    }

    @Override
    public String toString() {
        return _toString(request);
    }

    public static List<Token> tokens() {
        return Collections.singletonList(new Token(Token.Type.OBJECT, Request.class));
    }

    public static RequestMessage construct(Object... data) {
        return construct(RequestMessage.class, new Class[] {Request.class}, data);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RequestMessage
                && request == ((RequestMessage) o).request;
    }
}
