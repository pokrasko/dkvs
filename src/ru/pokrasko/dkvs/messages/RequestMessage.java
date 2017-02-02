package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Operation;
import ru.pokrasko.dkvs.replica.Request;

public class RequestMessage<R> extends Message {
    private Request<R> request;

    public RequestMessage(Request<R> request) {
        this.request = request;
    }

    public Request<R> getRequest() {
        return request;
    }

    public Operation<R> getOperation() {
        return request.getOperation();
    }

    public int getClientId() {
        return request.getClientId();
    }

    public int getRequestNumber() {
        return request.getRequestNumber();
    }

    @Override
    public String toString() {
        return "Request " + request;
    }
}
