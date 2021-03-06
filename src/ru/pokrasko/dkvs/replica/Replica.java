package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.SafeRunnable;

import java.util.HashMap;
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
    private int lastNormalViewNumber;

    private Log log;
    private Map<Integer, Integer> clientTable = new HashMap<>();

    private int opNumber;
    private int commitNumber;

    public Replica(int replicaNumber, int id, Log recoveryLog) {
        this.replicaNumber = replicaNumber;
        this.failureNumber = (replicaNumber - 1) / 2;

        this.id = id;

        if (recoveryLog != null) {
            this.opNumber = recoveryLog.size();
            this.log = recoveryLog;
        } else {
            this.log = new Log();
        }
    }

    @Override
    public void run() {
        if (commitNumber < opNumber) {
            log.cut(commitNumber);
            opNumber = commitNumber;
        }

        start();
    }

    public int getReplicaNumber() {
        return replicaNumber;
    }

    public int getExcludingQuorumNumber() {
        return failureNumber;
    }

    public int getIncludingQuorumNumber() {
        return failureNumber + 1;
    }

    public int getId() {
        return id;
    }

    public Status getStatus() {
        return status;
    }

    public int getViewNumber() {
        return viewNumber;
    }

    public int getLastNormalViewNumber() {
        return lastNormalViewNumber;
    }

    public Log getLog() {
        return log;
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

    public int getNewPrimaryId(int viewNumber) {
        return viewNumber % replicaNumber;
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

    public int appendLog(Request<?, ?> request) {
        log.add(request);
        clientTable.put(request.getClientId(), ++opNumber);
        return opNumber;
    }

    public Request<?, ?> getRequestToCommit() {
        return getRequestByOpNumber(++commitNumber);
    }

    public void setStatus(Status status, int viewNumber) {
        this.status = status;
        this.viewNumber = viewNumber;
        if (status == Status.NORMAL) {
            this.lastNormalViewNumber = viewNumber;
        }
    }

    public Request<?, ?> getLatestClientRequest(int clientId) {
        return clientTable.containsKey(clientId) ? getRequestByOpNumber(clientTable.get(clientId)) : null;
    }

    public Request<?, ?> getRequestByOpNumber(int opNumber) {
        return log.get(opNumber);
    }

    public void updateLog(Log log, int opNumber) {
        this.log.addAll(log, this.commitNumber, opNumber);
        this.opNumber = opNumber;
    }
}
