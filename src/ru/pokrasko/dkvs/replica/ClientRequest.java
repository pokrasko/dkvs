package ru.pokrasko.dkvs.replica;

public class ClientRequest<R> {
    private Request<R> request;
    private boolean isExecuted;
    private R result;

    public ClientRequest(Request<R> request) {
        this.request = request;
    }

    public void execute(R result) {
        isExecuted = true;
        this.result = result;
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

    public boolean isExecuted() {
        return isExecuted;
    }

    public R getResult() {
        return result;
    }
}
