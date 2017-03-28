package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.quorums.VlnkQuorum;
import ru.pokrasko.dkvs.service.Result;

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
    private int lastNormalViewNumber;

    private Log log;
    private Map<Integer, Integer> clientTable = new HashMap<>();

    private int opNumber;
    private int commitNumber;

    public Replica(int replicaNumber, int id, int recoveryOpNumber, Log recoveryLog) {
        this.replicaNumber = replicaNumber;
        this.failureNumber = (replicaNumber - 1) / 2;

        this.id = id;

        if (recoveryOpNumber > 0 && recoveryLog.size() == recoveryOpNumber) {
            this.commitNumber = this.opNumber = recoveryOpNumber;
            this.log = recoveryLog;
        } else {
            this.log = new Log();
        }
    }

    @Override
    public void run() {
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

    public void setCommitNumber(int commitNumber) {
        assert commitNumber > this.commitNumber && commitNumber <= opNumber;
        this.commitNumber = commitNumber;
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

//    public Result<?> getLatestResult(int clientId) {
//        if (!clientTable.containsKey(clientId)) {
//            return null;
//        }
//        Request<?, ?> latestRequest = getLatestClientRequest(clientId);
//
//        return latestRequest.equals(request) ? latestRequest.getResult() : null;
//    }

    public int appendLog(Request<?, ?> request) {
        log.add(request);
        clientTable.put(request.getClientId(), ++opNumber);
        return opNumber;
    }

//    public Result<?> getResult(int opNumber) {
//        return getRequestByOpNumber(opNumber).getResult();
//    }
//
//    public int getOpClientId(int opNumber) {
//        return getRequestByOpNumber(opNumber).getClientId();
//    }
//
//    public int getOpRequestNumber(int opNumber) {
//        return getRequestByOpNumber(opNumber).getRequestNumber();
//    }

//    public void startStateTransfer(long timeout) {}
//
//    public void startViewChange() {
//        viewNumber++;
//        status = Status.VIEW_CHANGE;
//    }
//
//    public void startViewChange(int viewNumber) {
//        this.viewNumber = viewNumber;
//        status = Status.VIEW_CHANGE;
//    }
//
//    public void finishViewChangeAsPrimary(Log log, int opNumber, int commitNumber) {
//        this.log = log;
//        this.opNumber = opNumber;
//        this.commitNumber = commitNumber;
//        status = Status.NORMAL;
//        lastNormalViewNumber = viewNumber;
//    }

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

    public void recovery(VlnkQuorum.Data vlnkData) {
        assert vlnkData.getOpNumber() - opNumber == vlnkData.getLog().size();
        log.addAll(vlnkData.getLog());
        opNumber = vlnkData.getOpNumber();
        commitNumber = vlnkData.getCommitNumber();
    }

    public void updateLnk(Log log, int opNumber, int commitNumber) {
        log.addAll(log, this.commitNumber, opNumber);
        this.opNumber = opNumber;
        this.commitNumber = commitNumber;
    }
}
