package ru.pokrasko.dkvs;

import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.runnables.Accepter;
import ru.pokrasko.dkvs.runnables.Connector;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {
    private final int id;
    private final Properties properties;

    private List<AtomicBoolean> isConnectedIn;
    private List<AtomicBoolean> isConnectedOut;
    private BlockingQueue<Message> inQueue;
    private BlockingQueue<Message> outQueue;

    Server(int id, Properties properties) {
        this.id = id;
        this.properties = properties;

        isConnectedIn = new ArrayList<>(Collections.nCopies(properties.getServerAmount(), new AtomicBoolean()));
        isConnectedOut = new ArrayList<>(Collections.nCopies(properties.getServerAmount(), new AtomicBoolean()));
        inQueue = new LinkedBlockingQueue<>();
        outQueue = new LinkedBlockingQueue<>();
    }

    void run() {
        InetSocketAddress address = getServerAddress(id);
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(address.getPort(), 50, address.getAddress());
        } catch (IOException e) {
            logError("Couldn't open a server socket %s (" + e.getMessage() + ")");
            return;
        }
        new Thread(new Accepter(this, serverSocket)).run();
        for (int i = 0; i < properties.getServerAmount(); i++) {
            new Thread(new Connector(this, i)).run();
        }
    }


    private void logError(String message) {
        System.err.println(String.format(message, "at server #" + (id + 1)));
    }


    public int getId() {
        return id + 1;
    }

    public InetSocketAddress getServerAddress(int id) {
        return properties.getServerAddress(id);
    }

    public int getTimeout() {
        return properties.getTimeout();
    }

    public boolean getConnectedIn(int id) {
        return isConnectedIn.get(id).get();
    }

    public boolean setConnectedIn(int id, boolean value) {
        return isConnectedIn.get(id).getAndSet(value);
    }

    public boolean getConnectedOut(int id) {
        return isConnectedOut.get(id).get();
    }

    public boolean setConnectedOut(int id, boolean value) {
        return isConnectedOut.get(id).getAndSet(value);
    }
    
    public Message getIncomingMessage() throws InterruptedException {
        return inQueue.poll();
    }
    
    public void putIncomingMessage(Message message) throws InterruptedException {
        inQueue.add(message);
    }
    
    public Message getOutgoingMessage() throws InterruptedException {
        return outQueue.poll(getTimeout() / 2, TimeUnit.MILLISECONDS);
    }
    
    public void putOutgoingMessage(Message message) throws InterruptedException {
        outQueue.put(message);
    }
}
