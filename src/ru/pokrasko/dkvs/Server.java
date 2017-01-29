package ru.pokrasko.dkvs;

import ru.pokrasko.dkvs.runnables.SocketAccepter;
import ru.pokrasko.dkvs.runnables.SocketConnecter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {
    private final int id;
    private final Properties properties;

    private List<AtomicBoolean> isConnectedIn;
    private List<AtomicBoolean> isConnectedOut;
    private ConcurrentLinkedQueue<Message> inQueue;
    private ConcurrentLinkedQueue<Message> outQueue;

    Server(int id, Properties properties) {
        this.id = id;
        this.properties = properties;

        isConnectedIn = new ArrayList<>(Collections.nCopies(properties.getServerAmount(), new AtomicBoolean()));
        isConnectedOut = new ArrayList<>(Collections.nCopies(properties.getServerAmount(), new AtomicBoolean()));
        inQueue = new ConcurrentLinkedQueue<>();
        outQueue = new ConcurrentLinkedQueue<>();
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
        new Thread(new SocketAccepter(this, serverSocket)).run();
        for (int i = 0; i < properties.getServerAmount(); i++) {
            new Thread(new SocketConnecter(this, i)).run();
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
    
    public Message getIncomingMessage() {
        return inQueue.poll();
    }
    
    public void putIncomingMessage(Message message) {
        inQueue.add(message);
    }
    
    public Message getOutgoingMessage() {
        return outQueue.poll();
    }
    
    public void putOutgoingMessage(Message message) {
        outQueue.add(message);
    }
}
