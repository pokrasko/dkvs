package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.replica.Replica;
import ru.pokrasko.dkvs.replica.Request;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

class Processor implements Runnable {
    private BlockingQueue<Message> in;
    private List<BlockingQueue<Message>> serverOuts;
    private ConcurrentMap<Integer, BlockingQueue<Message>> clientOuts;

    private Replica replica;

    private int timeout;

    Processor(BlockingQueue<Message> in,
              List<BlockingQueue<Message>> serverOuts, ConcurrentMap<Integer, BlockingQueue<Message>> clientOuts,
              Server server) {
        this.in = in;
        this.serverOuts = serverOuts;
        this.clientOuts = clientOuts;

        timeout = server.getTimeout();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Message message = in.poll(timeout / 2, TimeUnit.MILLISECONDS);

                if (message instanceof PingMessage
                        || message instanceof ViewedMessage
                        && !replica.checkMessageForModernity((ViewedMessage) message)) {
                    continue;
                }

                if (message instanceof RequestMessage) {
                    if (!replica.isPrimary()) {
                        continue;
                    }

                    RequestMessage<?> requestMessage = (RequestMessage<?>) message;
                    Request<?> request = requestMessage.getRequest();
                    if (!replica.isRequestNew(request)) {
                        if (!replica.isRequestOld(request)) {
                            sendMessageToClient(request.getClientId(), new ReplyMessage<>(replica.getViewNumber(),
                                    request.getRequestNumber(),
                                    replica.getLatestResult(request)));
                        }
                        continue;
                    }

                    int opNumber = replica.putRequest(request);
                    broadcastMessageToReplicas(new PrepareMessage(replica.getViewNumber(),
                            request,
                            opNumber,
                            replica.getCommitNumber()));
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private void sendMessageToReplica(int thatId, Message message) throws InterruptedException {
        serverOuts.get(thatId).put(message);
    }

    private void broadcastMessageToReplicas(Message message) throws InterruptedException {
        for (BlockingQueue<Message> queue : serverOuts) {
            queue.put(message);
        }
    }

    private void sendMessageToClient(int clientId, Message message) throws InterruptedException {
        if (!clientOuts.containsKey(clientId)) {
            return;
        }

        clientOuts.get(clientId).put(message);
    }
}
