package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Server;
import ru.pokrasko.dkvs.messages.Message;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Accepter extends SafeRunnable {
    private ServerSocket serverSocket;

    private List<AtomicBoolean> isConnectedIn;
    private BlockingQueue<Message> incomingQueue;
    private int timeout;

    private List<Receiver> receivers = new LinkedList<>();
    private List<Thread> receiverThreads = new LinkedList<>();

    public Accepter(Server server, ServerSocket serverSocket) {
        this.serverSocket = serverSocket;

        this.isConnectedIn = server.getIsConnectedIn();
        this.incomingQueue = server.getIncomingMessageQueue();
        this.timeout = server.getTimeout();
    }

    @Override
    public void run() {
        start();

        while (isRunning()) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                stop();
                continue;
            }

            try {
                Receiver receiver = new Receiver(socket, timeout, isConnectedIn, incomingQueue, this);
                Thread receiverThread = new Thread(receiver);
                receiverThread.start();
                receivers.add(receiver);
                receiverThreads.add(receiverThread);
            } catch (IOException e) {
                System.err.println("Couldn't run a socket reader (" + e.getMessage() + ")");
            }
        }

        try {
            for (Thread receiverThread : receiverThreads) {
                receiverThread.join();
            }
        } catch (InterruptedException e) {
            System.err.println("INTERNAL OOPS!");
        }
    }

    @Override
    public void stop() {
        super.stop();
        try {
            serverSocket.close();
        } catch (IOException ignored) {}
        receivers.forEach(Receiver::stop);
    }

    void removeReceiver(Receiver receiver, Thread receiverThread) {
        receivers.remove(receiver);
        receiverThreads.remove(receiverThread);
    }
}
