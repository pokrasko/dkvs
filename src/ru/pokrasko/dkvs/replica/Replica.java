package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.messages.ViewedMessage;

import java.util.List;
import java.util.Map;

public class Replica {
    public enum Status {
        NORMAL, VIEW_CHANGE, RECOVERY
    };

    private int failureNumber;
    private int amount;
    private int id;

    private int viewNumber;
    private Status status;

    private List<Operation<?>> log;
    private Map<Integer, ClientRequest<?>> clientTable;

    private int opNumber = -1;
    private int commitNumber;

    public int getViewNumber() {
        return viewNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    public boolean isPrimary() {
        return id == viewNumber % amount;
    }

    public int getPrimaryId() {
        return viewNumber % amount;
    }

    public boolean checkMessageForModernity(ViewedMessage message) {
        if (message.getViewNumber() > viewNumber) {
            startStateTransfer();
        }
        return message.getViewNumber() == viewNumber;
    }

    public boolean isRequestOld(Request<?> request) {
        int clientId = request.getClientId();
        return clientTable.containsKey(clientId)
                && request.getRequestNumber() < clientTable.get(clientId).getRequestNumber();
    }

    public boolean isRequestNew(Request<?> request) {
        int clientId = request.getClientId();
        return !clientTable.containsKey(clientId)
                || request.getRequestNumber() > clientTable.get(clientId).getRequestNumber();
    }

    public Object getLatestResult(Request request) {
        int clientId = request.getClientId();
        if (!clientTable.containsKey(clientId)) {
            return null;
        }

        return clientTable.get(clientId).getResult();
    }

    public <R> int putRequest(Request<R> request) {
        if (!isPrimary()) {
            throw new IllegalStateException();
        }

        log.add(request.getOperation());
        clientTable.put(request.getClientId(), new ClientRequest<>(request));
        return ++opNumber;
    }


    private int getAmount() {
        return amount;
    }

    private void startStateTransfer() {}
}
