package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.quorums.VlnkQuorum;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Replica;
import ru.pokrasko.dkvs.replica.Request;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

class Processor implements Runnable {
    private Server server;

    private BlockingQueue<Message> in;
    private List<BlockingDeque<Message>> serverOuts;
    private BlockingDeque<Message> thisOut;
    private ConcurrentMap<Integer, BlockingQueue<Message>> clientOuts;
    private int timeout;

    private Replica replica;
    private Support support;

    Processor(BlockingQueue<Message> in,
              List<BlockingDeque<Message>> serverOuts,
              ConcurrentMap<Integer, BlockingQueue<Message>> clientOuts,
              Support support, Server server) {
        this.server = server;

        this.in = in;
        this.serverOuts = serverOuts;
        this.clientOuts = clientOuts;
        timeout = server.getTimeout();

        this.replica = support.getReplica();
        this.support = support;

        this.thisOut = serverOuts.get(replica.getId());
    }

    @Override
    public void run() {
        support.startIdentification(replica.getId());

        while (!Thread.interrupted()) {
            try {
                Message message = in.poll(timeout / 2, TimeUnit.MILLISECONDS);

//                resendWaitingMessages();
//
//                if (message == null) {
//                    continue;
//                }
//
//                if (message instanceof NewReplicaMessage) {
//                    NewReplicaMessage newReplicaMessage = (NewReplicaMessage) message;
//                    sendMessageToReplica(newReplicaMessage.getId(), new AcceptedMessage(replica.getId()));
//                } else if (message instanceof AcceptedMessage) {
//                    AcceptedMessage acceptedMessage = (AcceptedMessage) message;
//                    server.setAccepted(acceptedMessage.getId());
//                }
//
//                if (!replica.isRunning()
//                        || message instanceof ViewedMessage
//                            && ((ViewedMessage) message).getViewNumber() < replica.getViewNumber()) {
//                    continue;
//                }
//
//                if (!replica.isPrimary() && !support.isPrimaryConnected()) {
//                    support.startViewChange();
//                    continue;
//                }
//
//                Replica.Status status = replica.getStatus();
//                if (status != Replica.Status.RECOVERY) {
//                    if (status != Replica.Status.VIEW_CHANGE) {                 // NORMAL PROTOCOL
//                        if (message instanceof ViewedMessage
//                                && !support.checkMessageForModernity((ViewedMessage) message)) {
//                            resendWaitingMessages();
//                            continue;
//                        }
//                        if (message instanceof RequestMessage) {
//                            if (!replica.isPrimary()) {
//                                continue;
//                            }
//
//                            RequestMessage requestMessage = (RequestMessage) message;
//                            Request<?, ?> request = requestMessage.getRequest();
//                            if (!replica.isRequestNew(request)) {
//                                if (replica.isRequestOld(request)) {
//                                    sendMessageToClient(request.getClientId(), new ReplyMessage(replica.getViewNumber(),
//                                            request.getRequestNumber(),
//                                            replica.getLatestResult(request)));
//                                }
//                                continue;
//                            }
//
//                            int opNumber = replica.appendLog(request);
//                            broadcastMessageToReplicas(new PrepareMessage(replica.getViewNumber(),
//                                    opNumber,
//                                    replica.getCommitNumber(),
//                                    request));
//                        } else {
//                            if (replica.isPrimary()) {
//                                broadcastMessageToReplicas(new CommitMessage(replica.getViewNumber(),
//                                        replica.getCommitNumber()));
//                            }
//
//                            if (message instanceof PrepareMessage) {
//                                if (replica.isPrimary()) {
//                                    continue;
//                                }
//
//                                PrepareMessage prepareMessage = (PrepareMessage) message;
//                                if (prepareMessage.getOpNumber() > replica.getOpNumber() + 1) {
//                                    replica.startStateTransferUpgrade(5 * timeout);
//                                } else {
//                                    if (prepareMessage.getOpNumber() == replica.getOpNumber() + 1) {
//                                        replica.appendLog(prepareMessage.getRequest());
//                                    }
//                                    sendMessageToPrimary(new PrepareOkMessage(replica.getViewNumber(),
//                                            replica.getOpNumber(),
//                                            replica.getId()));
//                                }
//
//                                int commitNumber = prepareMessage.getCommitNumber();
//                                while (replica.getCommitNumber() < commitNumber
//                                        && support.commit(replica.getCommitNumber() + 1)) {
//                                }
//                            } else if (message instanceof PrepareOkMessage) {
//                                PrepareOkMessage prepareOkMessage = (PrepareOkMessage) message;
//                                int opNumber = prepareOkMessage.getOpNumber();
//                                if (replica.getCommitNumber() < opNumber) {
//                                    int newCommitNumber = support.checkPrepareQuorum(opNumber,
//                                            prepareOkMessage.getReplicaId());
//                                    System.out.println("New commit number is " + newCommitNumber);
//                                    while (replica.getCommitNumber() < newCommitNumber) {
//                                        int commitNumber = replica.getCommitNumber() + 1;
//                                        support.commit(commitNumber);
//                                        sendMessageToClient(replica.getOpClientId(commitNumber),
//                                                new ReplyMessage(replica.getViewNumber(),
//                                                        replica.getOpRequestNumber(commitNumber),
//                                                        replica.getResult(commitNumber)));
//                                    }
//                                }
//                            }
//                        }
//                    }
//                                                                                // VIEW CHANGE MESSAGE
//                    if (message instanceof StartViewChangeMessage) {
//                        StartViewChangeMessage startViewChangeMessage = (StartViewChangeMessage) message;
//                        int viewNumber = startViewChangeMessage.getViewNumber();
//                        if (viewNumber > replica.getViewNumber()) {
//                            support.startViewChange(viewNumber);
//                        } else if (status == Replica.Status.NORMAL) {
//                            continue;
//                        }
//                        if (support.checkChangeQuorum(viewNumber, startViewChangeMessage.getReplicaId())) {
//                            support.continueViewChange();
//                        }
//                    } else if (message instanceof DoViewChangeMessage) {
//                        DoViewChangeMessage doViewChangeMessage = (DoViewChangeMessage) message;
//                        int viewNumber = doViewChangeMessage.getViewNumber();
//                        if (viewNumber > replica.getViewNumber()) {
//                            support.startViewChange(viewNumber);
//                        } else if (status == Replica.Status.NORMAL) {
//                            continue;
//                        }
//                        if (!replica.isPrimary()) {
//                            continue;
//                        }
//                        if (support.checkNewPrimaryQuorum(doViewChangeMessage)) {
//                            NewPrimaryQuorum quorum = support.getNewPrimaryQuorum();
//                            broadcastMessageToReplicas(new StartViewMessage(doViewChangeMessage.getViewNumber(),
//                                    quorum.getLogFrom(),
//                                    quorum.getOpNumber(),
//                                    quorum.getCommitNumber()));
//                        }
//                    }
//                }

                // No messages
                if (message == null) {
                    handleNullMessage();
                    continue;
                }

                // Accepting new replicas is handled outside the replica protocol
                if (message instanceof NewReplicaMessage) {
                    NewReplicaMessage newReplicaMessage = (NewReplicaMessage) message;
                    sendMessageToReplica(newReplicaMessage.getId(), new AcceptedMessage(replica.getId()));
                } else if (message instanceof AcceptedMessage) {
                    AcceptedMessage acceptedMessage = (AcceptedMessage) message;
                    server.setAccepted(acceptedMessage.getId());
                }

                // Any replica protocol message is handled only when the replica is running
                if (!replica.isRunning() && !support.checkTimeToStart()) {
                    resendWaitingMessages();
                    continue;
                }
                Replica.Status status = replica.getStatus();

                // View change is started when:
                if (status == Replica.Status.NORMAL || status == Replica.Status.VIEW_CHANGE) {
                    // 1. this view primary is not connected (thus it is not responding),
                    // new view number is the next one
                    if (!replica.isPrimary() && !support.isPrimaryConnected()) {
                        support.setStatus(Replica.Status.VIEW_CHANGE, replica.getViewNumber() + 1);
                        continue;
                    // 2. StartViewChange or DoViewChange message is received
                    // and its view number is larger than this one's,
                    // new view number is got from the message
                    } else if (message instanceof StartViewChangeMessage || message instanceof DoViewChangeMessage) {
                        int viewNumber = ((ViewNumberMessage) message).getViewNumber();
                        if (viewNumber > replica.getViewNumber()) {
                            support.setStatus(Replica.Status.VIEW_CHANGE, viewNumber);
                            continue;
                        }
                    }
                }

                // If there is running state transfer, the replica handles only NewState messages
                if (support.getStateTransferState() == Support.STState.UPGRADE) {
                    if (message instanceof NewStateMessage) {
                        NewStateMessage newStateMessage = (NewStateMessage) message;
                        if (newStateMessage.getViewNumber() < replica.getViewNumber()
                                || newStateMessage.getOpNumber() < replica.getOpNumber()) {
                            continue;
                        }

                        support.setStatus(replica.getStatus(), newStateMessage.getViewNumber());

                    }
                }

                try {
                    // If the replica is in normal status, then messages of all protocols are handled
                    if (status == Replica.Status.NORMAL) {
                        // RECOVERY PROTOCOL (IN NORMAL STATUS)
                        if (message instanceof RecoveryMessage) {
                            RecoveryMessage recoveryMessage = (RecoveryMessage) message;
                            sendMessageToReplica(recoveryMessage.getReplicaId(),
                                    new RecoveryResponseMessage(replica.getViewNumber(),
                                            recoveryMessage.getNonce(),
                                            replica.isPrimary()
                                                    ? replica.getLog().getAfter(recoveryMessage.getOpNumber())
                                                    : null,
                                            replica.getOpNumber(),
                                            replica.getCommitNumber(),
                                            replica.getId()));
                        }

                        // VIEW CHANGE PROTOCOL (IN NORMAL STATUS)
                        // If a view change protocol message is received
                        // and its view number is larger than this one's
                        // then this replica's view number is updated
                        // and status is changed to normal (if a StartView is received) or view change (otherwise)
                        if (message instanceof StartViewChangeMessage || message instanceof DoViewChangeMessage
                                || message instanceof StartViewMessage) {
                            int viewNumber = ((ViewNumberMessage) message).getViewNumber();
                            if (viewNumber <= replica.getViewNumber()) {
                                continue;
                            }

                            if (message instanceof StartViewMessage) {
                                StartViewMessage startViewMessage = (StartViewMessage) message;
                                handleNewView(viewNumber,
                                        startViewMessage.getLog(),
                                        startViewMessage.getOpNumber(),
                                        startViewMessage.getCommitNumber(),
                                        replica.getNewPrimaryId(viewNumber));
                            } else {
                                support.setStatus(Replica.Status.VIEW_CHANGE, viewNumber);
                            }
                        }

                        // NORMAL PROTOCOL
                        // Checking the message's view number
                        if (message instanceof ViewNumberMessage) {
                            int viewNumber = ((ViewNumberMessage) message).getViewNumber();
                            // If the message's view number is larger than this one's, then state transfer is started
                            if (viewNumber > replica.getViewNumber()) {
                                support.startStateTransferUpgrade(replica.getNewPrimaryId(viewNumber));
                            }
                            // Only messages with the same view number as this one's are handled according to the protocol
                            if (viewNumber != replica.getViewNumber()) {
                                continue;
                            }
                        }

                        // A primary handles Request and PrepareOk normal protocol messages
                        if (replica.isPrimary()) {
                            if (message instanceof RequestMessage) {
                                Request<?, ?> request = ((RequestMessage) message).getRequest();
                                if (!replica.isRequestNew(request)) {
                                    if (!replica.isRequestOld(request)
                                            && replica.getLatestClientRequest(request.getClientId())
                                                .getResult() != null) {
                                        sendMessageToClient(request.getClientId(), new ReplyMessage(replica.getViewNumber(),
                                                request.getRequestNumber(),
                                                request.getResult()));
                                    }
                                    continue;
                                }

                                int opNumber = replica.appendLog(request);
                                support.addPrepareMessage(new PrepareMessage(replica.getViewNumber(),
                                        request,
                                        opNumber,
                                        replica.getCommitNumber()));
                            } else if (message instanceof PrepareOkMessage) {
                                PrepareOkMessage prepareOkMessage = (PrepareOkMessage) message;
                                int commitNumber = support.checkPrepareOkQuorum(prepareOkMessage.getReplicaId(),
                                        prepareOkMessage.getOpNumber());
                                if (commitNumber > 0) {
                                    for (int i = replica.getCommitNumber() + 1; i <= commitNumber; i++) {
                                        System.err.println("The primary is going to commit request #" + i + ": "
                                                + replica.getRequestByOpNumber(i));
                                        commit(i);
                                    }
                                }
//                            } else {
//                                handleWrongMessage(message, status);
                            }
                            // A backup handles Prepare and Commit messages
                        } else {
                            if (!(message instanceof PrepareMessage || message instanceof CommitMessage)) {
//                                handleWrongMessage(message, status);
                                continue;
                            }

                            if (message instanceof PrepareMessage) {
                                PrepareMessage prepareMessage = (PrepareMessage) message;
                                int opNumber = prepareMessage.getOpNumber();
                                if (opNumber <= replica.getOpNumber()) {
                                    sendMessageToPrimary(new PrepareOkMessage(replica.getViewNumber(),
                                            opNumber,
                                            replica.getId()));
                                } else if (opNumber == replica.getOpNumber() + 1) {
                                    replica.appendLog(prepareMessage.getRequest());
                                    sendMessageToPrimary(new PrepareOkMessage(replica.getViewNumber(),
                                            replica.getOpNumber(),
                                            replica.getId()));
                                    if (support.getStateTransferState() == Support.STState.UPDATE) {
                                        support.gotNeeded(opNumber);
                                    }
                                } else {
                                    support.startStateTransferUpdate(replica.getPrimaryId(), opNumber - 1);
                                }
                            }

                            int commitNumber = ((CommitNumberMessage) message).getCommitNumber();
                            support.checkForCommit(commitNumber);
                        }

                        // If the replica is in view change status, then only view change protocol messages are handled
                    } else if (status == Replica.Status.VIEW_CHANGE) {
                        // VIEW CHANGE PROTOCOL (IN VIEW CHANGE STATUS)
                        if (!(message instanceof StartViewChangeMessage || message instanceof DoViewChangeMessage
                                || message instanceof StartViewMessage)) {
//                            handleWrongMessage(message, status);
                            continue;
                        }

                        int viewNumber = ((ViewNumberMessage) message).getViewNumber();
                        if (viewNumber > replica.getViewNumber()) {
                            if (message instanceof StartViewMessage) {
                                StartViewMessage startViewMessage = (StartViewMessage) message;
                                handleNewView(viewNumber,
                                        startViewMessage.getLog(),
                                        startViewMessage.getOpNumber(),
                                        startViewMessage.getCommitNumber(),
                                        replica.getNewPrimaryId(viewNumber));
                            } else {
                                support.setStatus(Replica.Status.VIEW_CHANGE, viewNumber);
                            }
                        } else if (viewNumber == replica.getViewNumber()) {
                            if (message instanceof StartViewChangeMessage) {
                                StartViewChangeMessage startViewChangeMessage = (StartViewChangeMessage) message;
                                support.checkStartViewChangeQuorum(startViewChangeMessage.getReplicaId());
                            } else if (message instanceof DoViewChangeMessage) {
                                if (replica.isPrimary()) {
                                    DoViewChangeMessage doViewChangeMessage = (DoViewChangeMessage) message;
                                    VlnkQuorum.Data vlnkData = support.checkVlnkQuorum(
                                            doViewChangeMessage.getReplicaId(),
                                            doViewChangeMessage.getLastNormalViewNumber(),
                                            doViewChangeMessage.getLog(),
                                            doViewChangeMessage.getOpNumber(),
                                            doViewChangeMessage.getCommitNumber());
                                    if (vlnkData != null) {
                                        if (handleNewView(replica.getViewNumber(), vlnkData.getLog(),
                                                vlnkData.getOpNumber(), vlnkData.getCommitNumber(),
                                                support.getMaxViewNumberReplicaId())) {
                                            broadcastMessageToReplicas(new StartViewMessage(replica.getViewNumber(),
                                                    replica.getLog()
                                                            .getSuffix(DoViewChangeMessage.VIEW_CHANGE_LOG_SUFFIX_SIZE),
                                                    replica.getOpNumber(),
                                                    replica.getCommitNumber()));
                                        }
                                    }
//                                } else {
//                                    handleWrongMessage(message, status);
                                }
                            } else {
                                StartViewMessage startViewMessage = (StartViewMessage) message;
                                handleNewView(viewNumber,
                                        startViewMessage.getLog(),
                                        startViewMessage.getOpNumber(),
                                        startViewMessage.getCommitNumber(),
                                        replica.getNewPrimaryId(viewNumber));
                            }
                        }

                        // If the replica is in recovery status, then only recovery protocol messages are handled
                    } else {
                        // RECOVERY PROTOCOL (IN RECOVERY STATUS)
                        if (!(message instanceof RecoveryResponseMessage)) {
//                            handleWrongMessage(message, status);
                            continue;
                        }

                        RecoveryResponseMessage recoveryResponseMessage = (RecoveryResponseMessage) message;
                        if (recoveryResponseMessage.getNonce() != support.getRecoveryNonce()) {
                            continue;
                        }

                        VlnkQuorum.Data vlnkData = support.checkVlnkQuorum(recoveryResponseMessage.getReplicaId(),
                                recoveryResponseMessage.getViewNumber(),
                                recoveryResponseMessage.getLog(),
                                recoveryResponseMessage.getOpNumber(),
                                recoveryResponseMessage.getCommitNumber());
                        if (vlnkData != null) {
                            recovery(vlnkData);
                        }
                    }
                } finally {
                    // After each handled message, the replica resends waiting messages
                    resendWaitingMessages();
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void sendMessageToReplica(int thatId, Message message) throws InterruptedException {
        putToQueue(serverOuts.get(thatId), message);
    }

    private void sendMessageToPrimary(Message message) throws InterruptedException {
        sendMessageToReplica(replica.getPrimaryId(), message);
    }

    private void broadcastMessageToReplicas(Message message) throws InterruptedException {
        for (BlockingDeque<Message> queue : serverOuts) {
            if (queue.equals(thisOut)) {
                continue;
            }
            putToQueue(queue, message);
        }
    }

    private void putToQueue(BlockingDeque<Message> deque, Message message) throws InterruptedException {
        if (deque.peek() != message) {
            deque.put(message);
        }
    }

    private void sendMessageToClient(int clientId, Message message) throws InterruptedException {
        if (!clientOuts.containsKey(clientId)) {
            return;
        }

        clientOuts.get(clientId).put(message);
    }

    private void resendWaitingMessages() throws InterruptedException {
//        List<Waiting> waitings = support.getWaitingList();
//        for (Waiting waiting : waitings) {
//            if (waiting.getSender() != -1) {
//                sendMessageToReplica(waiting.getSender(), waiting.getMessage());
//            } else {
//                broadcastMessageToReplicas(waiting.getMessage());
//            }
//        }
        List<Map.Entry<Integer, Message>> waitingMessages = support.checkWaiting();
        if (waitingMessages != null) {
            for (Map.Entry<Integer, Message> waitingMessage : waitingMessages) {
                if (waitingMessage.getKey() == -1) {
                    broadcastMessageToReplicas(waitingMessage.getValue());
                } else {
                    sendMessageToReplica(waitingMessage.getKey(), waitingMessage.getValue());
                }
            }
        }
    }

    private void handleNullMessage() throws InterruptedException {
        resendWaitingMessages();
    }

    private void commit(int opNumber) throws InterruptedException {
        support.commit(opNumber);
        Request<?, ?> request = replica.getRequestByOpNumber(opNumber);
        if (!replica.isRequestOld(request)) {
            sendMessageToClient(request.getClientId(),
                    new ReplyMessage(replica.getViewNumber(),
                            request.getRequestNumber(),
                            request.getResult()));
        }
    }

    private boolean handleNewView(int viewNumber, Log log, int opNumber, int commitNumber,
                                  int stReceiverId) throws InterruptedException {
        support.setStatus(Replica.Status.NORMAL, viewNumber);

        if (opNumber - replica.getCommitNumber() > log.size()) {
            support.startStateTransferUpgrade(stReceiverId);
            return false;
        } else {
            replica.updateLog(log, opNumber);
            for (int i = replica.getCommitNumber() + 1; i <= commitNumber; i++) {
                System.err.println("The replica in new view is going to commit request #" + i + ": "
                        + replica.getRequestByOpNumber(i));
                commit(i);
            }
            return true;
        }
    }

    private void recovery(VlnkQuorum.Data vlnkData) throws InterruptedException {
        assert vlnkData.getOpNumber() - replica.getCommitNumber() == vlnkData.getLog().size();

        support.setStatus(Replica.Status.NORMAL, vlnkData.getViewNumber());

        replica.updateLog(vlnkData.getLog(), vlnkData.getOpNumber());
        for (int i = replica.getCommitNumber() + 1; i <= vlnkData.getCommitNumber(); i++) {
            System.err.println("The recovering replica in new view is going to commit request #" + i + ": "
                    + replica.getRequestByOpNumber(i));
            commit(i);
        }
    }

//    private void handleWrongMessage(Message message, Replica.Status status) {
//        System.out.println("Ignoring message \"" + message + "\" in status " + status
//                + ", is this primary: " + replica.isPrimary());
//    }
}
