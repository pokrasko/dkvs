package ru.pokrasko.dkvs.client;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.ReplyMessage;
import ru.pokrasko.dkvs.messages.RequestMessage;
import ru.pokrasko.dkvs.replica.Request;
import ru.pokrasko.dkvs.service.Operation;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class Processor extends SafeRunnable {
    private Client client;

    private Thread incomingProcessorThread;
    private Thread outgoingProcessorThread;

    private BlockingQueue<Message> in;
    private List<BlockingQueue<Message>> outs;
    private int timeout;

    private int id;
    private int requestNumber;
    private Request<?, ?> currentRequest;

    Processor(BlockingQueue<Message> in, List<BlockingQueue<Message>> outs, Client client) {
        this.client = client;

        this.in = in;
        this.outs = outs;
        this.timeout = client.getTimeout();

        id = client.getId();
    }

    @Override
    public void run() {
        start();

        Thread incomingThread = new Thread(new IncomingProcessor());
        Thread outgoingThread = new Thread(new OutgoingProcessor());
        incomingThread.start();
        outgoingThread.start();
        incomingProcessorThread = incomingThread;
        outgoingProcessorThread = outgoingThread;
        try {
            incomingProcessorThread.join();
            outgoingProcessorThread.join();
        } catch (InterruptedException ignored) {
            stop();
        }
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        if (incomingProcessorThread != null) {
            incomingProcessorThread.interrupt();
        }

        if (outgoingProcessorThread != null) {
            outgoingProcessorThread.interrupt();
        }

        return true;
    }


    private class IncomingProcessor implements Runnable {
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    Message message = in.poll(timeout / 2, TimeUnit.MILLISECONDS);

                    if (message == null) {
                        continue;
                    }

                    if (message instanceof ReplyMessage) {
                        ReplyMessage replyMessage = (ReplyMessage) message;
                        if (replyMessage.getRequestNumber() != requestNumber) {
                            continue;
                        }
                        client.setResult(replyMessage.getResult());
                        currentRequest = null;
                    }
                } catch (InterruptedException ignored) {
                    break;
                }
            }
        }
    }


    private class OutgoingProcessor implements Runnable {
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                if (currentRequest == null) {
                    Operation<?, ?> operation = client.pollOperation();

                    if (operation == null) {
                        try {
                            Thread.sleep(Client.PROCESS_TIMEOUT);
                        } catch (InterruptedException ignored) {
                            break;
                        }
                        continue;
                    } else {
                        client.getResultParser().setLastOperation(operation);
                        currentRequest = Request.fromOperation(operation, id, ++requestNumber);
                    }
                }

                try {
                    broadcastMessage(new RequestMessage(currentRequest));
                    Thread.sleep(timeout);
                } catch (InterruptedException ignored) {
                    break;
                }
            }
        }

        private void broadcastMessage(Message message) throws InterruptedException {
            for (BlockingQueue<Message> queue : outs) {
                queue.put(message);
            }
        }
    }
}
