package ru.pokrasko.dkvs;

import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.runnables.Accepter;
import ru.pokrasko.dkvs.runnables.Connector;
import ru.pokrasko.dkvs.runnables.Processor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {
    private final int id;
    private final Properties properties;

    private List<AtomicBoolean> isConnectedIn;
    private List<AtomicBoolean> isConnectedOut;
    private BlockingQueue<Message> inQueue = new LinkedBlockingQueue<>();
    private List<BlockingQueue<Message>> outQueues;

    private Accepter accepter;
    private List<Connector> connectors = new ArrayList<>();

    private Thread accepterThread;
    private List<Thread> connectorThreads = new ArrayList<>();
    private Thread processorThread;

    Server(int id, Properties properties) {
        this.id = id;
        this.properties = properties;

        int amount = properties.getServerAmount();
        isConnectedIn = new ArrayList<>(Collections.nCopies(amount, new AtomicBoolean()));
        isConnectedOut = new ArrayList<>(Collections.nCopies(amount, new AtomicBoolean()));
        outQueues = new ArrayList<>(Collections.nCopies(amount, new LinkedBlockingQueue<>()));
    }

    void start() {
        InetSocketAddress address = getServerAddress(id);
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(address.getPort(), 50, address.getAddress());
        } catch (IOException e) {
            logError("Couldn't open a server socket %s (" + e.getMessage() + ")");
            return;
        }

        accepter = new Accepter(this, serverSocket);
        accepterThread = new Thread(accepter);
        accepterThread.start();
        for (int i = 0; i < properties.getServerAmount(); i++) {
            if (i == id) {
                continue;
            }

            Connector connector = new Connector(this, i);
            Thread connectorThread = new Thread(connector);
            connectorThread.start();
            connectors.add(connector);
            connectorThreads.add(connectorThread);
        }
        processorThread = new Thread(new Processor(inQueue, outQueues, this));
        processorThread.start();
    }

    void stop() {
        if (accepter != null) {
            accepter.stop();
        }
        connectors.forEach(Connector::stop);
        processorThread.interrupt();

        try {
            if (accepterThread != null) {
                accepterThread.join();
            }
            if (processorThread != null) {
                processorThread.join();
            }
            for (Thread connectorThread : connectorThreads) {
                connectorThread.join();
            }
        } catch (InterruptedException ignored) {
            System.err.println("OOPS!");
        }
    }


    private void logError(String message) {
        System.err.println(String.format(message, "at server #" + (id + 1)));
    }


    public int getId() {
        return id;
    }

    public InetSocketAddress getServerAddress(int id) {
        return properties.getServerAddress(id);
    }

    public int getTimeout() {
        return properties.getTimeout();
    }

    public List<AtomicBoolean> getIsConnectedIn() {
        return isConnectedIn;
    }

    public List<AtomicBoolean> getIsConnectedOut() {
        return isConnectedOut;
    }

    public BlockingQueue<Message> getIncomingMessageQueue() {
        return inQueue;
    }

    public BlockingQueue<Message> getOutgoingMessageQueue(int id) {
        return outQueues.get(id);
    }
}
