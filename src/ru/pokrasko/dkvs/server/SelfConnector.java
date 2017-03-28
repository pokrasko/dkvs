package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.messages.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class SelfConnector extends SafeRunnable {
    private BlockingQueue<Message> in;
    private BlockingQueue<Message> out;
    private int timeout;

    SelfConnector(Server server) {
        this.in = server.getIncomingMessageQueue();
        this.out = server.getOutgoingServerMessageQueue(server.getId());
        this.timeout = server.getTimeout();
    }

    @Override
    public void run() {
        start();

        while (isRunning()) {
            try {
                Message message = out.poll(timeout / 2, TimeUnit.MILLISECONDS);
                if (message != null) {
                    in.put(message);
                }
            } catch (InterruptedException ignored) {
                stop();
            }
        }
    }
}
