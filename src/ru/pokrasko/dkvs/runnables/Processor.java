package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Main;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PingMessage;
import ru.pokrasko.dkvs.messages.PongMessage;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Processor implements Runnable {
    private ConcurrentLinkedQueue<Message> in;
    private ConcurrentLinkedQueue<Message> out;

    public Processor(ConcurrentLinkedQueue<Message> in, ConcurrentLinkedQueue<Message> out) {
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            Message message = in.poll();
            if (message == null) {
                try {
                    Thread.sleep(Main.SLEEP_TIMEOUT);
                } catch (InterruptedException e) {
                    return;
                }
            } else if (message instanceof PingMessage) {
                out.add(new PongMessage());
            }
        }
    }
}
