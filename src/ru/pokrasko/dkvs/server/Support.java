package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.waiting.*;
import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.quorums.*;
import ru.pokrasko.dkvs.replica.Replica;
import ru.pokrasko.dkvs.service.Service;
import ru.pokrasko.dkvs.waiting.Waiting;

import java.util.List;
import java.util.Map;

class Support extends SafeRunnable {
//    private enum ChangeStatus {
//        NO, FIRST, SECOND
//    }
    enum STState {
        NO, UPDATE, UPGRADE
    }

    private Replica replica;

    private Service service = new Service();

//    private Set<ru.pokrasko.dkvs.server.Waiting> waitingSet = new HashSet<>();
//    private AcceptedQuorum acceptedQuorum;
//    private PrepareQuorum prepareQuorum;
//    private ChangeQuorum changeQuorum;
//    private NewPrimaryQuorum newPrimaryQuorum;

//    private ChangeStatus changeStatus;


    private Waiting waiting;
    private AcceptedQuorum acceptedQuorum;
    private Quorum<?> quorum;
    private StartViewChangeQuorum startViewChangeQuorum;

    private boolean vcSecondStage;

    private long recoveryNonce;

    private STState stateTransferState;

    private int timeout;


    Support(Replica replica, int timeout) {
        this.replica = replica;
//        this.acceptedQuorum = new AcceptedQuorum(replica, replica.getReplicaNumber());
//        this.prepareQuorum = new PrepareQuorum(replica);
//        this.changeQuorum = new ChangeQuorum(replica, replica.getReplicaNumber());
//        this.newPrimaryQuorum = new NewPrimaryQuorum(replica, replica.getReplicaNumber());
        this.timeout = timeout;

        this.acceptedQuorum = new AcceptedQuorum(replica, replica.getReplicaNumber());
    }

    Replica getReplica() {
        return replica;
    }

    public STState getStateTransferState() {
        return stateTransferState;
    }

    @Override
    public void run() {
        start();
    }

    boolean commit(int numberToCommit) {
        if (numberToCommit != replica.getCommitNumber() + 1 || numberToCommit > replica.getOpNumber()) {
            return false;
        }

        service.commit(replica.getRequestToCommit());
        return true;
    }

//    boolean checkMessageForModernity(ViewedMessage message) {
//        if (message.getViewNumber() > replica.getViewNumber()) {
//            startStateTransferUpgrade(0);
//        }
//        return message.getViewNumber() == replica.getViewNumber();
//    }

    long getRecoveryNonce() {
        return recoveryNonce;
    }

    void startStateTransferUpdate(int responder, int lastNeeded) {
        if (stateTransferState == STState.UPDATE) {
            ((StateTransferUpdateWaiting) waiting).newNeeded(lastNeeded);
        } else {
            System.err.println("Starting update state transfer");
            stateTransferState = STState.UPDATE;
            waiting = new StateTransferUpdateWaiting(new GetStateMessage(replica.getViewNumber(),
                    replica.getOpNumber(),
                    replica.getId()), responder, lastNeeded, 0L, timeout / 2);
        }
    }

    void startStateTransferUpgrade(int responder) {
        System.err.println("Starting upgrade state transfer");
        stateTransferState = STState.UPGRADE;
        waiting = new StateTransferWaiting(new GetStateMessage(replica.getViewNumber(),
                replica.getOpNumber(),
                replica.getId()), responder, 0L, timeout / 2);
//        replica.startStateTransfer(timeout);
    }

    void newNeeded(int opNumber) {
        assert stateTransferState == STState.UPDATE && replica.getId() != replica.getPrimaryId();
        ((StateTransferUpdateWaiting) waiting).newNeeded(opNumber);
    }

    void gotNeeded(int opNumber) {
        assert stateTransferState == STState.UPDATE && replica.getId() != replica.getPrimaryId();
        if (((StateTransferUpdateWaiting) waiting).gotNeeded(opNumber)) {
            stateTransferState = STState.NO;
            waiting = null;
        }
    }

    void finishStateTransfer() {
        stateTransferState = STState.NO;
        if (replica.getStatus() == Replica.Status.NORMAL && replica.isPrimary()) {
            waiting = new PrepareCommitWaiting(replica.getViewNumber(), replica.getOpNumber(),
                    timeout / 2, timeout / 2);
        }
    }

//    void startViewChange() {
//        replica.startViewChange();
//        if (changeStatus == ChangeStatus.FIRST) {
//            removeWaiting(StartViewChangeMessage.class);
//        } else if (changeStatus == ChangeStatus.SECOND) {
//            removeWaiting(DoViewChangeMessage.class);
//        }
//        addWaitingBroadcast(new StartViewChangeMessage(replica.getViewNumber(),
//                replica.getId()), 0, timeout / 2);
//        changeStatus = ChangeStatus.FIRST;
//    }

//    void startViewChange(int viewNumber) {
//        replica.startViewChange(viewNumber);
//        if (changeStatus == ChangeStatus.FIRST) {
//            removeWaiting(StartViewChangeMessage.class);
//        } else if (changeStatus == ChangeStatus.SECOND) {
//            removeWaiting(DoViewChangeMessage.class);
//        }
//        addWaitingBroadcast(new StartViewChangeMessage(replica.getViewNumber(),
//                replica.getId()), 0, timeout / 2);
//        changeStatus = ChangeStatus.FIRST;
//    }

//    void continueViewChange() {
//        removeWaiting(StartViewChangeMessage.class);
//        addWaitingFromPrimary(new DoViewChangeMessage(replica.getViewNumber(),
//                    replica.getLogFrom(0),
//                    replica.getLastNormalViewNumber(),
//                    replica.getOpNumber(),
//                    replica.getCommitNumber(),
//                    replica.getId()),
//                0, timeout / 2);
//        changeStatus = ChangeStatus.SECOND;
//    }
//
//    void finishViewChangeAsPrimary(Log log, int opNumber, int commitNumber) {
//        replica.finishViewChangeAsPrimary(log, opNumber, commitNumber);
//        changeStatus = ChangeStatus.NO;
//    }
//
//    List<ru.pokrasko.dkvs.server.Waiting> getWaitingList() {
//        return waitingSet.stream().filter(ru.pokrasko.dkvs.server.Waiting::isTimedOut).collect(Collectors.toList());
//    }

//    void addWaitingFromPrimary(Message message, long timeout, long period) {
//        waitingSet.add(new ru.pokrasko.dkvs.server.Waiting(message, replica.getPrimaryId(), timeout, period));
//    }

    void startIdentification(int id) {
        waiting = new SimpleWaiting(new NewReplicaMessage(id), true, 0L, timeout);
    }

//    void removeWaiting(Class<? extends Message> messageClass) {
//        ru.pokrasko.dkvs.server.Waiting removed = null;
//        for (ru.pokrasko.dkvs.server.Waiting waiting : waitingSet) {
//            if (messageClass.isAssignableFrom(waiting.getMessage().getClass())) {
//                removed = waiting;
//                break;
//            }
//        }
//        if (removed != null) {
//            waitingSet.remove(removed);
//        }
//    }

//    void clearWaiting() {
//        waitingSet.clear();
//    }

    boolean isPrimaryConnected() {
        return acceptedQuorum.isConnected(replica.getPrimaryId());
    }

    void checkAcceptedQuorum(int id, boolean isConnecting) {
        if (isConnecting && acceptedQuorum.connect(id)) {
            if (replica.getOpNumber() == 0) {
                // Starting the replica from scratch: setting status to NORMAL
                waiting = null;
                setStatus(Replica.Status.NORMAL, 0);
            } else {
                // Recovering the replica, last known operation number is recoveryOpNumber,
                // setting status to RECOVERY, broadcasting Recovery message
                RecoveryMessage recoveryMessage = new RecoveryMessage(replica.getId(), replica.getOpNumber());
                recoveryNonce = recoveryMessage.getNonce();
                waiting = new SimpleWaiting(recoveryMessage, true, 0L, timeout / 2);
                setStatus(Replica.Status.RECOVERY, 0);
            }
            replica.run();
        } else if (!isConnecting && acceptedQuorum.disconnect(id)) {
            replica.stop();
        }
    }

//    int checkPrepareQuorum(int opNumber, int id) {
//        return prepareQuorum.checkQuorum(opNumber, id);
//    }
//
//    boolean checkChangeQuorum(int viewNumber, int id) {
//        return changeQuorum.checkQuorum(viewNumber, id);
//    }
//
//    boolean checkNewPrimaryQuorum(DoViewChangeMessage message) {
//        return newPrimaryQuorum.checkQuorum(message);
//    }
//
//    NewPrimaryQuorum getNewPrimaryQuorum() {
//        return newPrimaryQuorum;
//    }



    void setStatus(Replica.Status status, int viewNumber) {
        replica.setStatus(status, viewNumber);

        if (status == Replica.Status.NORMAL && replica.isPrimary()) {
            waiting = new PrepareCommitWaiting(viewNumber, replica.getOpNumber(), timeout / 2, timeout / 2);
            quorum = new PrepareOkQuorum(replica.getExcludingQuorumNumber(), replica.getReplicaNumber());
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
            for (int i = replica.getCommitNumber() + 1; i < newCommitNumber; i++) {
                commit(i);
            }
            replica.setCommitNumber(newCommitNumber);
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
