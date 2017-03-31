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
    }

    @Override
    public void run() {
        support.startIdentification(replica.getId());

        while (!Thread.interrupted()) {
            try {
                Message message = in.poll(timeout / 2, TimeUnit.MILLISECONDS);

                // No messages
                if (message == null) {
                    handleNullMessage();
                    continue;
                }

                // Accepting new replicas is handled outside the replica protocol
                if (message instanceof NewReplicaMessage) {
                    int thatId = ((NewReplicaMessage) message).getId();
                    sendMessageToReplica(thatId, new AcceptedMessage(replica.getId()));
                    if (!support.isReplicaAccepted(thatId)) {
                        sendMessageToReplica(thatId, new NewReplicaMessage(replica.getId()));
                    }
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

                try {
                    // Checking this view primary for a response (if there is no one, starting view change)
                    if (!checkPrimaryForResponse()) {
                        continue;
                    }

                    if (!(message instanceof ProtocolMessage)) {
                        continue;
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

                    // View change is started when:
                    if (status == Replica.Status.NORMAL || status == Replica.Status.VIEW_CHANGE) {
                        // If StartViewChange or DoViewChange message is received
                        // and its view number is larger than this one's, view change is also started and
                        // new view number is got from the message
                        if (message instanceof StartViewChangeMessage || message instanceof DoViewChangeMessage) {
                            int viewNumber = ((ViewNumberMessage) message).getViewNumber();
                            if (viewNumber > replica.getViewNumber()) {
                                support.setStatus(Replica.Status.VIEW_CHANGE, viewNumber);
                                continue;
                            }
                        }
                    }

                    // If the replica is in normal status, then messages of all protocols are handled
                    if (status == Replica.Status.NORMAL) {
                        // RECOVERY PROTOCOL (IN NORMAL STATUS)
                        if (message instanceof RecoveryMessage) {
                            RecoveryMessage recoveryMessage = (RecoveryMessage) message;
                            sendMessageToReplica(recoveryMessage.getReplicaId(),
                                    new RecoveryResponseMessage(replica.getViewNumber(),
                                            recoveryMessage.getNonce(),
                                            replica.getId(),
                                            replica.isPrimary()
                                                    ? replica.getLog().getAfter(recoveryMessage.getOpNumber())
                                                    : null,
                                            replica.getOpNumber(),
                                            replica.getCommitNumber()));
                        } else if (((ProtocolMessage) message).getProtocol() == ProtocolMessage.Protocol.RECOVERY) {
                            continue;
                        }

                        // VIEW CHANGE PROTOCOL (IN NORMAL STATUS)
                        // If a view change protocol message is received
                        // and its view number is larger than this one's
                        // then this replica's view number is updated
                        // and status is changed to normal (if a StartView is received)
                        if (message instanceof StartViewMessage) {
                            int viewNumber = ((ViewNumberMessage) message).getViewNumber();
                            if (viewNumber <= replica.getViewNumber()) {
                                continue;
                            }

                            StartViewMessage startViewMessage = (StartViewMessage) message;
                            handleNewView(viewNumber,
                                    startViewMessage.getLog(),
                                    startViewMessage.getOpNumber(),
                                    startViewMessage.getCommitNumber(),
                                    replica.getNewPrimaryId(viewNumber));
                        } else if (((ProtocolMessage) message).getProtocol() == ProtocolMessage.Protocol.VIEW_CHANGE) {
                            continue;
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
                        if (((ProtocolMessage) message).getProtocol() != ProtocolMessage.Protocol.NORMAL) {
                            continue;
                        }

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
                                        commit(i);
                                    }
                                }
                            } else {
                                handleWrongMessage(message, status, replica.getViewNumber());
                            }
                            // A backup handles Prepare and Commit messages
                        } else {
                            if (!(message instanceof CommitNumberMessage)) {
                                if (!(message instanceof RequestMessage)) {
                                    handleWrongMessage(message, status, replica.getViewNumber());
                                }
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
                        if (((ProtocolMessage) message).getProtocol() != ProtocolMessage.Protocol.VIEW_CHANGE) {
                            continue;
                        }

                        // VIEW CHANGE PROTOCOL (IN VIEW CHANGE STATUS)
                        int viewNumber = ((ViewNumberMessage) message).getViewNumber();
                        if (viewNumber > replica.getViewNumber()) {
                            if (message instanceof StartViewMessage) {
                                StartViewMessage startViewMessage = (StartViewMessage) message;
                                handleNewView(viewNumber,
                                        startViewMessage.getLog(),
                                        startViewMessage.getOpNumber(),
                                        startViewMessage.getCommitNumber(),
                                        replica.getNewPrimaryId(viewNumber));
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
                                } else {
                                    handleWrongMessage(message, status, replica.getViewNumber());
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
                        if (((ProtocolMessage) message).getProtocol() != ProtocolMessage.Protocol.RECOVERY
                                || message instanceof RecoveryMessage) {
                            continue;
                        }

                        // RECOVERY PROTOCOL (IN RECOVERY STATUS)
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
                            System.out.println("The replica recovered");
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
            putToQueue(queue, message);
        }
    }

    private void broadcastMessageToUnAnsweredReplicas(Message message, List<Boolean> answered)
            throws InterruptedException {
        for (int i = 0; i < replica.getReplicaNumber(); i++) {
            if (i == replica.getId()) {
                continue;
            }

            if (!answered.get(i)) {
                putToQueue(serverOuts.get(i), message);
            }
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
        List<Map.Entry<Integer, Message>> waitingMessages = support.checkWaiting();
        if (waitingMessages != null) {
            for (Map.Entry<Integer, Message> waitingMessage : waitingMessages) {
                if (waitingMessage.getKey() == -1) {
                    if (waitingMessage.getValue() instanceof NewReplicaMessage) {
                        broadcastMessageToUnAnsweredReplicas(waitingMessage.getValue(), support.getAcceptedList());
                    } else {
                        broadcastMessageToReplicas(waitingMessage.getValue());
                    }
                } else {
                    sendMessageToReplica(waitingMessage.getKey(), waitingMessage.getValue());
                }
            }
        }
    }

    private void handleNullMessage() throws InterruptedException {
        // The only sender to a backup in normal protocol is a primary, so the primary is likely off
        if (replica.isRunning()) {
            checkPrimaryForResponse();
        }
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
            support.commit(i);
        }
    }

    private void handleWrongMessage(Message message, Replica.Status status, int viewNumber) {
        System.out.println("Ignoring message \"" + message + "\" in status " + status
                + ", view #" + viewNumber + ", is this primary: " + replica.isPrimary());
    }

    private boolean checkPrimaryForResponse() {
        // View change is started if this view primary is not connected (thus it is not responding),
        // new view number is the next one
        if (!replica.isPrimary() && !support.isReplicaAccepted(replica.getPrimaryId())) {
            support.setStatus(Replica.Status.VIEW_CHANGE, replica.getViewNumber() + 1);
            return false;
        } else {
            return true;
        }
    }
}
