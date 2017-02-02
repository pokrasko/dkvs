package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Server;
import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.replica.Replica;
import ru.pokrasko.dkvs.replica.Request;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class Processor implements Runnable {
    private BlockingQueue<Message> in;
    private List<BlockingQueue<Message>> outs;

    private Replica replica;

    private int timeout;

    public Processor(BlockingQueue<Message> in, List<BlockingQueue<Message>> outs, Server server) {
        this.in = in;
        this.outs = outs;
        timeout = server.getTimeout();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Message message = in.poll(timeout / 2, TimeUnit.MILLISECONDS);

                if (message instanceof ViewedMessage
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
//                            outs.put(new ReplyMessage<>(replica.getViewNumber(),
//                                    request.getRequestNumber(),
//                                    replica.getLatestResult(request)));
                        }
                        continue;
                    }

                    int opNumber = replica.putRequest(request);
                    outs.put(new PrepareMessage(replica.getViewNumber(), request, opNumber, replica.getCommitNumber()));
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
