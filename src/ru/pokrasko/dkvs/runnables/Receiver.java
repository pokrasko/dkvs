package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.parsers.MessageParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class Receiver extends SafeRunnable {
    private Socket socket;
    private BufferedReader reader;

    private List<AtomicBoolean> isConnectedIn;
    private BlockingQueue<Message> queue;

    private MessageParser messageParser;

    private Accepter accepter;

    Receiver(Socket socket, int timeout, List<AtomicBoolean> isConnectedIn, BlockingQueue<Message> queue,
             Accepter accepter)
            throws IOException {
        this.socket = socket;
        this.socket.setSoTimeout(timeout);
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        this.isConnectedIn = isConnectedIn;
        this.queue = queue;

        this.messageParser = new MessageParser();

        this.accepter = accepter;
    }

    @Override
    public void run() {
        start();

        int thatId;
        try {
            thatId = Integer.parseInt(reader.readLine());
            System.out.println("Received connection from #" + (thatId + 1));
            isConnectedIn.get(thatId).set(true);
        } catch (IOException e) {
            System.err.println("Couldn't read server id on connection from " + socket.getRemoteSocketAddress()
                    + " (" + e.getMessage() + ")");
            stop();
            return;
        }

        while (isRunning()) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    stop();
                } else {
                    System.out.println("Received message from #" + (thatId + 1) + ": " + messageParser.parse(line));
                    queue.put(messageParser.parse(line));
                }
            } catch (InterruptedException | IOException e) {
                stop();
            }
        }

        System.out.println("Closing incoming connection from #" + (thatId + 1));
        isConnectedIn.get(thatId).set(false);
    }

    @Override
    public synchronized void stop() {
        if (!isRunning()) {
            return;
        }
        super.stop();

        try {
            socket.close();
        } catch (IOException ignored) {}
        if (accepter.isRunning()) {
            accepter.removeReceiver(this, Thread.currentThread());
        }
    }
}
