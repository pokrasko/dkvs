package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.service.Operation;
import ru.pokrasko.dkvs.replica.Request;

public class RequestMessage extends Message {
    private Request<?, ?> request;

    public RequestMessage(Request<?, ?> request) {
        this.request = request;
    }

    public Request<?, ?> getRequest() {
        return request;
    }

    public Operation<?, ?> getOperation() {
        return request.getOperation();
    }

    @Override
    public String toString() {
        return "Request " + request;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RequestMessage
                && request == ((RequestMessage) o).request;
    }
}
