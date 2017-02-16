package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.replica.Replica;
import ru.pokrasko.dkvs.replica.Request;

import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

class Processor implements Runnable {
    private BlockingQueue<Message> in;
    private List<BlockingDeque<Message>> serverOuts;
    private ConcurrentMap<Integer, BlockingQueue<Message>> clientOuts;
    private int timeout;

    private Replica replica;

    Processor(BlockingQueue<Message> in,
              List<BlockingDeque<Message>> serverOuts,
              ConcurrentMap<Integer, BlockingQueue<Message>> clientOuts,
              Replica replica, Server server) {
        this.in = in;
        this.serverOuts = serverOuts;
        this.clientOuts = clientOuts;
        timeout = server.getTimeout();

        this.replica = replica;
    }

    @Override
    public void run() {
        replica.setWaitingForResponse(new NewReplicaMessage(replica.getId()), -1);

        while (!Thread.interrupted()) {
            try {
                Message message = in.poll(timeout / 2, TimeUnit.MILLISECONDS);

                if (message == null) {
                    continue;
                }

                if (message instanceof NewReplicaMessage) {
                    NewReplicaMessage newReplicaMessage = (NewReplicaMessage) message;
                    sendMessageToReplica(newReplicaMessage.getId(), new AcceptedMessage(replica.getId()));
                } else if (message instanceof AcceptedMessage) {
                    AcceptedMessage acceptedMessage = (AcceptedMessage) message;
                    replica.checkAcceptedQuorum(acceptedMessage.getId(), true);
                }

                if (!replica.isRunning()) {
                    Message waitingForResponse = replica.getWaitingForResponse();
                    if (waitingForResponse != null) {
                        broadcastMessageToReplicas(replica.getWaitingForResponse());
                    }
                    continue;
                } else {
                    resendWaitingMessage();

                    if (message instanceof ViewedMessage
                            && !replica.checkMessageForModernity((ViewedMessage) message)) {
                        resendWaitingMessage();
                        continue;
                    }
                }

                if (replica.getStatus() == Replica.Status.NORMAL) {
                    if (message instanceof RequestMessage) {
                        if (!replica.isPrimary()) {
                            continue;
                        }

                        RequestMessage requestMessage = (RequestMessage) message;
                        Request<?, ?> request = requestMessage.getRequest();
                        if (!replica.isRequestNew(request)) {
                            if (replica.isRequestOld(request)) {
                                sendMessageToClient(request.getClientId(), new ReplyMessage(replica.getViewNumber(),
                                        request.getRequestNumber(),
                                        replica.getLatestResult(request)));
                            }
                            continue;
                        }

                        int opNumber = replica.appendLog(request);
                        replica.initPrepareQuorum(opNumber);
                        broadcastMessageToReplicas(new PrepareMessage(replica.getViewNumber(),
                                opNumber,
                                replica.getCommitNumber(),
                                request));
                    } else {
                        if (replica.isPrimary()) {
                            broadcastMessageToReplicas(new CommitMessage(replica.getViewNumber(),
                                    replica.getCommitNumber()));
                        }

                        if (message instanceof PrepareMessage) {
                            if (replica.isPrimary()) {
                                continue;
                            }

                            PrepareMessage prepareMessage = (PrepareMessage) message;
                            if (prepareMessage.getOpNumber() > replica.getOpNumber() + 1) {
                                replica.startStateTransfer();
                                resendWaitingMessage();
                            } else {
                                if (prepareMessage.getOpNumber() == replica.getOpNumber() + 1) {
                                    replica.appendLog(prepareMessage.getRequest());
                                }
                                sendMessageToPrimary(new PrepareOkMessage(replica.getViewNumber(),
                                        replica.getOpNumber(),
                                        replica.getId()));
                            }
                        } else if (message instanceof PrepareOkMessage) {
                            PrepareOkMessage prepareOkMessage = (PrepareOkMessage) message;
                            int opNumber = prepareOkMessage.getOpNumber();
                            if (replica.getCommitNumber() < opNumber
                                    && replica.checkPrepareQuorum(opNumber, prepareOkMessage.getReplicaId())) {
                                replica.commit(opNumber);
                                sendMessageToClient(replica.getOpClientId(opNumber),
                                        new ReplyMessage(replica.getViewNumber(),
                                                replica.getOpRequestNumber(opNumber),
                                                replica.getResult(opNumber)));
                            }
                        }
                    }
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

    private void resendWaitingMessage() throws InterruptedException {
        if (replica.getWaitingForResponse() != null) {
            if (replica.getWaitingFrom() != -1) {
                sendMessageToReplica(replica.getWaitingFrom(), replica.getWaitingForResponse());
            } else {
                broadcastMessageToReplicas(replica.getWaitingForResponse());
            }
        }
    }
}
