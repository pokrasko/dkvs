package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Main;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PingMessage;
import ru.pokrasko.dkvs.messages.PongMessage;

import java.util.concurrent.BlockingQueue;

public class Processor implements Runnable {
    private BlockingQueue<Message> in;
    private BlockingQueue<Message> out;

    public Processor(BlockingQueue<Message> in, BlockingQueue<Message> out) {
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Message message = in.take();

                if (message instanceof PingMessage) {
                    out.put(new PongMessage());
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
