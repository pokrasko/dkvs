package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.Properties;
import ru.pokrasko.dkvs.messages.Message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Server extends SafeRunnable {
    private final int id;
    private final Properties properties;

    private BlockingQueue<Message> inQueue = new LinkedBlockingQueue<>();
    private List<BlockingQueue<Message>> serverOutQueues;
    private ConcurrentMap<Integer, BlockingQueue<Message>> clientOutQueues = new ConcurrentHashMap<>();

    private List<SafeRunnable> safeRunnables = new LinkedList<>();
    private List<Receiver> receivers = new LinkedList<>();
    private List<ClientSender> senders = new LinkedList<>();

    private Thread processorThread;
    private List<Thread> safeThreads = new LinkedList<>();
    private List<Thread> receiverThreads = new LinkedList<>();
    private List<Thread> senderThreads = new LinkedList<>();

    public Server(int id, Properties properties) {
        this.id = id;
        this.properties = properties;

        int amount = properties.getServerAmount();
        serverOutQueues = new ArrayList<>(Collections.nCopies(amount, new LinkedBlockingQueue<>()));
    }

    @Override
    public void run() {
        start();

        InetSocketAddress address = getServerAddress(id);
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(address.getPort(), 50, address.getAddress());
        } catch (IOException e) {
            logError("Couldn't open a server socket %s (" + e.getMessage() + ")");
            return;
        }

        Accepter accepter = new Accepter(this, serverSocket);
        Thread accepterThread = new Thread(accepter);
        accepterThread.start();
        safeRunnables.add(accepter);
        safeThreads.add(accepterThread);

        for (int i = 0; i < properties.getServerAmount(); i++) {
            if (i == id) {
                continue;
            }

            Connector connector = new Connector(i, this);
            Thread connectorThread = new Thread(connector);
            connectorThread.start();
            safeRunnables.add(connector);
            safeThreads.add(connectorThread);
        }

        Thread processorThread = new Thread(new Processor(inQueue, serverOutQueues, clientOutQueues, this));
        processorThread.start();
        this.processorThread = processorThread;
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        safeRunnables.forEach(SafeRunnable::stop);
        receivers.forEach(Receiver::stop);
        senders.forEach(ClientSender::stop);
        if (processorThread != null) {
            processorThread.interrupt();
        }

        try {
            for (Thread safeThread : safeThreads) {
                safeThread.join();
            }
            for (Thread receiverThread : receiverThreads) {
                receiverThread.join();
            }
            for (Thread senderThread : senderThreads) {
                senderThread.join();
            }
            if (processorThread != null) {
                processorThread.join();
            }
        } catch (InterruptedException ignored) {
            System.err.println("OOPS!");
        }

        return true;
    }


    private void logError(String message) {
        System.err.println(String.format(message, "at server #" + (id + 1)));
    }


    int getId() {
        return id;
    }

    InetSocketAddress getServerAddress(int id) {
        return properties.getServerAddress(id);
    }

    int getTimeout() {
        return properties.getTimeout();
    }


    BlockingQueue<Message> getIncomingMessageQueue() {
        return inQueue;
    }

    BlockingQueue<Message> getOutgoingServerMessageQueue(int id) {
        return serverOutQueues.get(id);
    }


    void registerReceiver(Receiver receiver, Thread receiverThread) {
        receivers.add(receiver);
        receiverThreads.add(receiverThread);
    }

    void registerSender(ClientSender sender, Thread senderThread) {
        BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
        clientOutQueues.put(sender.getThatId(), queue);
        sender.setQueue(queue);

        senders.add(sender);
        senderThreads.add(senderThread);
    }

    void unregisterReceiver(Receiver receiver, Thread receiverThread) {
        receivers.remove(receiver);
        receiverThreads.remove(receiverThread);
    }

    void unregisterSender(ClientSender sender, Thread senderThread) {
        clientOutQueues.remove(sender.getThatId());

        senders.remove(sender);
        senderThreads.remove(senderThread);
    }
}
