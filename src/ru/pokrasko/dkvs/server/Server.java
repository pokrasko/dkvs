package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.files.Properties;
import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.files.LogFileHandler;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Replica;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Server extends SafeRunnable {
    private final int id;
    private final Properties properties;

    private List<Boolean> isConnectedIn;
    private List<Boolean> isConnectedOut;
    private final Lock connectedLock = new ReentrantLock();

    private BlockingQueue<Message> inQueue = new LinkedBlockingQueue<>();
    private List<BlockingDeque<Message>> serverOutQueues = new ArrayList<>();
    private ConcurrentMap<Integer, BlockingQueue<Message>> clientOutQueues = new ConcurrentHashMap<>();

    private List<SafeRunnable> safeRunnables = new LinkedList<>();
    private List<Receiver> receivers = new LinkedList<>();
    private List<ClientSender> senders = new LinkedList<>();

    private Thread processorThread;
    private List<Thread> safeThreads = new LinkedList<>();
    private List<Thread> receiverThreads = new LinkedList<>();
    private List<Thread> senderThreads = new LinkedList<>();

    private Support support;

    Server(int id, Properties properties, LogFileHandler logFileHandler) {
        this.id = id;
        this.properties = properties;

        int amount = properties.getServerAmount();
        isConnectedIn = new ArrayList<>(Collections.nCopies(amount, false));
        isConnectedOut = new ArrayList<>(Collections.nCopies(amount, false));

        for (int i = 0; i < amount; i++) {
            serverOutQueues.add(new LinkedBlockingDeque<>());
        }

        Log recoveryLog = logFileHandler.readLog();
        support = new Support(new Replica(amount, id, recoveryLog), logFileHandler, getTimeout());
    }

    @Override
    public void run() {
        start();

        InetSocketAddress address = properties.getServerAddress(id);
        ServerSocket serverSocket;
        try {
            serverSocket = new ServerSocket(address.getPort(), 50, address.getAddress());
        } catch (IOException e) {
            logError("Couldn't open a server socket %s (" + e.getMessage() + ")");
            return;
        }

        Thread processorThread = new Thread(new Processor(inQueue, serverOutQueues, clientOutQueues, support, this));
        processorThread.start();
        this.processorThread = processorThread;

        Accepter accepter = new Accepter(this, serverSocket);
        Thread accepterThread = new Thread(accepter);
        accepterThread.start();
        safeRunnables.add(accepter);
        safeThreads.add(accepterThread);

        for (int i = 0; i < properties.getServerAmount(); i++) {
            if (i != id) {
                Connector connector = new Connector(i, this);
                Thread connectorThread = new Thread(connector);
                connectorThread.start();
                safeRunnables.add(connector);
                safeThreads.add(connectorThread);
            }
        }
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
        support.stop();

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

    synchronized BlockingQueue<Message> registerOutgoingClientMessageQueue(int id) {
        if (clientOutQueues.containsKey(id)) {
            return null;
        }

        BlockingQueue<Message> newQueue = new LinkedBlockingQueue<>();
        clientOutQueues.put(id, newQueue);
        return newQueue;
    }

    synchronized void unregisterOutgoingClientMessageQueue(int id) {
        clientOutQueues.remove(id);
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


    void setConnectedIn(int id) {
        setConnected(id, isConnectedIn);
    }

    void setConnectedOut(int id) {
        setConnected(id, isConnectedOut);
    }

    void resetConnectedIn(int id) {
        resetConnected(id, isConnectedIn, isConnectedOut);
    }

    void resetConnectedOut(int id) {
        resetConnected(id, isConnectedOut, isConnectedIn);
    }

    void setAccepted(int id) {
        synchronized (connectedLock) {
            support.checkAcceptedQuorum(id, true);
        }
    }

    private void setConnected(int id, List<Boolean> thisList) {
        synchronized (connectedLock) {
            thisList.set(id, true);
        }
    }

    private void resetConnected(int id, List<Boolean> thisList, List<Boolean> another) {
        synchronized (connectedLock) {
            if (!thisList.set(id, false)) {
                return;
            }
            if (another.get(id)) {
                support.checkAcceptedQuorum(id, false);
            }
        }
    }
}
