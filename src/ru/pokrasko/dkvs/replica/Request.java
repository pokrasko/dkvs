package ru.pokrasko.dkvs.replica;

public class Request<R> {
    private Operation<R> operation;
    private int clientId;
    private int requestNumber;

    public Request(Operation<R> operation, int clientId, int requestNumber) {
        this.operation = operation;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
    }

    public Operation<R> getOperation() {
        return operation;
    }

    public int getClientId() {
        return clientId;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    @Override
    public String toString() {
        return operation + " " + clientId + " " + requestNumber;
    }
}
