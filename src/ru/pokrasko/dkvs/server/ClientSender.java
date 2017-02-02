package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PingMessage;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class ClientSender extends SafeRunnable {
    private Server server;

    private Socket socket;
    private PrintWriter writer;

    private BlockingQueue<Message> queue;
    private int timeout;

    private int thatId;

    ClientSender(Socket socket, int thatId, Server server) throws IOException {
        this.server = server;

        this.socket = socket;
        try {
            writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException ignored) {}
            throw e;
        }

        timeout = server.getTimeout();

        this.thatId = thatId;
    }

    @Override
    public void run() {
        start();

        try {
            while (isRunning() && !writer.checkError()) {
                Message message = queue.poll(timeout / 2, TimeUnit.MILLISECONDS);
                if (message != null) {
                    System.out.println("Sending to client #" + thatId + " message: " + message);
                    writer.println(message);
                } else {
                    writer.println(new PingMessage());
                }
            }
        } catch (InterruptedException ignored) {
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}

            if (server.isRunning()) {
                server.unregisterSender(this, Thread.currentThread());
            }
        }
    }

    int getThatId() {
        return thatId;
    }

    void setQueue(BlockingQueue<Message> queue) {
        this.queue = queue;
    }
}
