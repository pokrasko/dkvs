package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.ViewedMessage;
import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.service.Result;
import ru.pokrasko.dkvs.service.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Replica extends SafeRunnable {
    public enum Status {
        NORMAL, VIEW_CHANGE, RECOVERY
    }

    private int replicaNumber;
    private int failureNumber;
    private int id;

    private Status status;
    private int viewNumber;
    private Message waitingForResponse;
    private int waitingFrom;

    private List<Request<?, ?>> log = new ArrayList<>();
    private Map<Integer, Integer> clientTable = new HashMap<>();

    private int opNumber;
    private int commitNumber;

    private AcceptedQuorum acceptedQuorum;
    private PrepareQuorum prepareQuorum;

    private Service service = new Service();

    public Replica(int replicaNumber, int id) {
        this.replicaNumber = replicaNumber;
        this.failureNumber = (replicaNumber - 1) / 2;

        this.id = id;

        acceptedQuorum = new AcceptedQuorum(this, replicaNumber);
        prepareQuorum = new PrepareQuorum(this, replicaNumber);
    }

    @Override
    public void run() {
        start();

        status = Status.NORMAL;
        viewNumber = 0;
        waitingForResponse = null;
    }

    int getExcludingQuorumNumber() {
        return failureNumber;
    }

    public int getId() {
        return id;
    }

    public Status getStatus() {
        return status;
    }

    public Message getWaitingForResponse() {
        return waitingForResponse;
    }

    public int getWaitingFrom() {
        return waitingFrom;
    }

    public void setWaitingForResponse(Message message, int from) {
        waitingForResponse = message;
        waitingFrom = from;
    }

    public int getViewNumber() {
        return viewNumber;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    public boolean isPrimary() {
        return id == viewNumber % replicaNumber;
    }

    public int getPrimaryId() {
        return viewNumber % replicaNumber;
    }

    public boolean checkMessageForModernity(ViewedMessage message) {
        if (message.getViewNumber() > viewNumber) {
            startStateTransfer();
        }
        return message.getViewNumber() == viewNumber;
    }

    public boolean isRequestOld(Request<?, ?> request) {
        int clientId = request.getClientId();
        return clientTable.containsKey(clientId)
                && request.getRequestNumber() < getLatestClientRequest(clientId).getRequestNumber();
    }

    public boolean isRequestNew(Request<?, ?> request) {
        int clientId = request.getClientId();
        return !clientTable.containsKey(clientId)
                || request.getRequestNumber() > getLatestClientRequest(clientId).getRequestNumber();
    }

    public Result<?> getLatestResult(Request<?, ?> request) {
        int clientId = request.getClientId();
        if (!clientTable.containsKey(clientId)) {
            return null;
        }
        Request<?, ?> latestRequest = getLatestClientRequest(clientId);

        return latestRequest.equals(request) ? latestRequest.getResult() : null;
    }

    public int appendLog(Request<?, ?> request) {
        log.add(request);
        clientTable.put(request.getClientId(), ++opNumber);
        return opNumber;
    }

    public void commit(int numberToCommit) {
        assert numberToCommit == commitNumber + 1;
        service.commit(getRequestByOpNumber(++commitNumber));
    }

    public Result<?> getResult(int opNumber) {
        return getRequestByOpNumber(opNumber).getResult();
    }

    public int getOpClientId(int opNumber) {
        return getRequestByOpNumber(opNumber).getClientId();
    }

    public int getOpRequestNumber(int opNumber) {
        return getRequestByOpNumber(opNumber).getRequestNumber();
    }

    public void checkAcceptedQuorum(int id, boolean isConnecting) {
        if (isConnecting && acceptedQuorum.connect(id)) {
            run();
        } else if (!isConnecting && acceptedQuorum.disconnect(id)) {
            waitingForResponse = null;
            stop();
        }
    }

    public void initPrepareQuorum(int opNumber) {
        prepareQuorum.initQuorum(opNumber);
    }

    public boolean checkPrepareQuorum(int opNumber, int id) {
        return prepareQuorum.checkQuorum(opNumber, id);
    }

    public void startStateTransfer() {}

    private Request<?, ?> getRequestByOpNumber(int opNumber) {
        return log.get(opNumber - 1);
    }

    private Request<?, ?> getLatestClientRequest(int clientId) {
        return getRequestByOpNumber(clientTable.get(clientId));
    }
}
