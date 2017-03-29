package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.files.LogFileHandler;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Request;
import ru.pokrasko.dkvs.waiting.*;
import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.quorums.*;
import ru.pokrasko.dkvs.replica.Replica;
import ru.pokrasko.dkvs.service.Service;
import ru.pokrasko.dkvs.waiting.Waiting;

import java.util.List;
import java.util.Map;

class Support extends SafeRunnable {
    enum STState {
        NO, UPDATE, UPGRADE
    }

    private Replica replica;

    private Service service = new Service();
    private LogFileHandler logFileHandler;

    private Waiting waiting;
    private AcceptedQuorum acceptedQuorum;
    private Quorum<?> quorum;
    private StartViewChangeQuorum startViewChangeQuorum;

    private boolean vcSecondStage;

    private long recoveryNonce;

    private STState stateTransferState;

    private long timeToStart;
    private int timeout;


    Support(Replica replica, LogFileHandler logFileHandler, int timeout) {
        this.replica = replica;
        this.logFileHandler = logFileHandler;

        this.timeout = timeout;

        this.acceptedQuorum = new AcceptedQuorum(replica, replica.getReplicaNumber());
    }

    Replica getReplica() {
        return replica;
    }

    STState getStateTransferState() {
        return stateTransferState;
    }

    @Override
    public void run() {
        start();
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        logFileHandler.close();

        return true;
    }

    boolean commit(int numberToCommit) {
        if (numberToCommit != replica.getCommitNumber() + 1 || numberToCommit > replica.getOpNumber()) {
            return false;
        }

        Request<?, ?> request = replica.getRequestToCommit();
        service.commit(request);
        logFileHandler.appendRequest(request);
        return true;
    }

    long getRecoveryNonce() {
        return recoveryNonce;
    }

    void startStateTransferUpdate(int responder, int lastNeeded) {
        if (stateTransferState == STState.UPDATE) {
            ((StateTransferUpdateWaiting) waiting).newNeeded(lastNeeded);
        } else {
            stateTransferState = STState.UPDATE;
            waiting = new StateTransferUpdateWaiting(new GetStateMessage(replica.getViewNumber(),
                    replica.getOpNumber(),
                    replica.getId()), responder, lastNeeded, 0L, timeout / 2);
        }
    }

    void startStateTransferUpgrade(int responder) {
        stateTransferState = STState.UPGRADE;
        waiting = new StateTransferWaiting(new GetStateMessage(replica.getViewNumber(),
                replica.getOpNumber(),
                replica.getId()), responder, 0L, timeout / 2);
    }

    void gotNeeded(int opNumber) {
        if (((StateTransferUpdateWaiting) waiting).gotNeeded(opNumber)) {
            stateTransferState = STState.NO;
            waiting = null;
        }
    }

    void startIdentification(int id) {
        timeToStart = System.currentTimeMillis() + timeout * 2;
        waiting = new SimpleWaiting(new NewReplicaMessage(id), true, 0L, timeout);
    }

    boolean checkTimeToStart() {
        if (System.currentTimeMillis() >= timeToStart && acceptedQuorum.check()) {
            // It's time to start the replica!
            startReplica();
            return true;
        } else {
            return false;
        }
    }

    boolean isReplicaAccepted(int replicaId) {
        return acceptedQuorum.isConnected(replicaId);
    }

    void checkAcceptedQuorum(int id, boolean isConnecting) {
        if (isConnecting && acceptedQuorum.connect(id) &&
                (System.currentTimeMillis() >= timeToStart || acceptedQuorum.checkFull())) {
            // It's time to start the replica!
            startReplica();
        } else if (!isConnecting && acceptedQuorum.disconnect(id)) {
            replica.stop();
        }
    }

    List<Boolean> getAcceptedList() {
        return acceptedQuorum.getConfirmed();
    }

    private void startReplica() {
        if (replica.getOpNumber() == 0) {
            // Starting the replica from scratch: setting status to NORMAL
            waiting = null;
            setStatus(Replica.Status.NORMAL, 0);
        } else {
            // Recovering the replica, last known operation number is recoveryOpNumber,
            // setting status to RECOVERY, broadcasting Recovery message
            RecoveryMessage recoveryMessage = new RecoveryMessage(replica.getId(), replica.getCommitNumber());
            recoveryNonce = recoveryMessage.getNonce();
            waiting = new SimpleWaiting(recoveryMessage, true, 0L, timeout / 2);
            setStatus(Replica.Status.RECOVERY, 0);
        }

        replica.run();
    }

    void setStatus(Replica.Status status, int viewNumber) {
        replica.setStatus(status, viewNumber);

        if (status == Replica.Status.NORMAL) {
            if (replica.isPrimary()) {
                waiting = new PrepareCommitWaiting(viewNumber, replica.getOpNumber(), timeout / 2, timeout / 2);
                quorum = new PrepareOkQuorum(replica.getExcludingQuorumNumber(), replica.getReplicaNumber());
            } else {
                waiting = null;
                quorum = null;
            }
        } else if (status == Replica.Status.VIEW_CHANGE) {
            waiting = new SimpleWaiting(new StartViewChangeMessage(viewNumber, replica.getId()), true, 0L, timeout / 2);
            startViewChangeQuorum = new StartViewChangeQuorum(replica.getExcludingQuorumNumber(),
                    replica.getReplicaNumber());
            if (replica.isPrimary()) {
                quorum = new DoViewChangeQuorum(replica.getExcludingQuorumNumber(), replica.getReplicaNumber(),
                        replica.getId(), replica.getLastNormalViewNumber(),
                        replica.getLog().getSuffix(DoViewChangeMessage.VIEW_CHANGE_LOG_SUFFIX_SIZE),
                        replica.getOpNumber(), replica.getCommitNumber());
            } else {
                quorum = null;
            }
            vcSecondStage = false;
        } else {
            waiting = new SimpleWaiting(new RecoveryMessage(replica.getId(), replica.getOpNumber()),
                    true, 0L, timeout / 2);
            quorum = new RecoveryResponseQuorum(replica.getIncludingQuorumNumber(), replica.getReplicaNumber());
        }
    }

    void addPrepareMessage(PrepareMessage message) {
        if (!(waiting instanceof PrepareCommitWaiting)) {
            System.err.println("This is not prepare/commit waiting (on adding a prepare message)!");
            return;
        }

        waiting.addMessage(message);
    }

    List<Map.Entry<Integer, Message>> checkWaiting() {
        return (waiting != null) ? waiting.check(replica.getPrimaryId()) : null;
    }

    void checkForCommit(int commitNumber) {
        int newCommitNumber = Math.min(commitNumber, replica.getOpNumber());
        if (newCommitNumber > replica.getCommitNumber()) {
            for (int i = replica.getCommitNumber() + 1; i <= newCommitNumber; i++) {
                commit(i);
            }
        }

        if (commitNumber > replica.getOpNumber()) {
            startStateTransferUpdate(replica.getPrimaryId(), commitNumber);
        }
    }

    int checkPrepareOkQuorum(int replicaId, int opNumber) {
        if (!(quorum instanceof PrepareOkQuorum)) {
            System.err.println("This is not PrepareOk quorum!");
            return 0;
        }

        int commitNumber = ((PrepareOkQuorum) quorum).check(replicaId, opNumber);
        if (commitNumber > 0) {
            if (!(waiting instanceof PrepareCommitWaiting)) {
                System.err.println("This is not prepare/commit waiting (on updating commit number)!");
                return 0;
            }

            ((PrepareCommitWaiting) waiting).updateCommitNumber(commitNumber);
        }
        return commitNumber;
    }

    void checkStartViewChangeQuorum(int replicaId) {
        if (!vcSecondStage && startViewChangeQuorum.check(replicaId)) {
            waiting = new SimpleWaiting(new DoViewChangeMessage(replica.getViewNumber(),
                    replica.getLog().getSuffix(DoViewChangeMessage.VIEW_CHANGE_LOG_SUFFIX_SIZE),
                    replica.getLastNormalViewNumber(),
                    replica.getOpNumber(),
                    replica.getCommitNumber(),
                    replica.getId()), false, 0L, timeout / 2);
        }
    }

    VlnkQuorum.Data checkVlnkQuorum(int replicaId, int viewNumber, Log log, int opNumber, int commitNumber) {
        if (!(quorum instanceof VlnkQuorum)) {
            System.err.println("This is not VLNK quorum!");
            return null;
        } else {
            return ((VlnkQuorum) quorum).check(replicaId, viewNumber, log, opNumber, commitNumber);
        }
    }

    Integer getMaxViewNumberReplicaId() {
        if (!(quorum instanceof DoViewChangeQuorum)) {
            System.err.println("This is not DoViewChange quorum!");
            return null;
        } else {
            return ((DoViewChangeQuorum) quorum).getMaxViewNumberReplicaId();
        }
    }
}
