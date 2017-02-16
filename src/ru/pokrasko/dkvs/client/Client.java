package ru.pokrasko.dkvs.client;

import ru.pokrasko.dkvs.Properties;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.parsers.OperationParser;
import ru.pokrasko.dkvs.parsers.ResultParser;
import ru.pokrasko.dkvs.service.Operation;
import ru.pokrasko.dkvs.service.Result;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class Client extends SafeRunnable {
    static final int PROCESS_TIMEOUT = 500;

    private final int id;
    private final Properties properties;

    private OperationParser operationParser = new OperationParser();
    private ResultParser resultParser = new ResultParser();

    private BlockingQueue<Message> inQueue = new LinkedBlockingQueue<>();
    private List<BlockingQueue<Message>> outQueues = new ArrayList<>();
    private volatile Operation<?, ?> operation;
    private Result<?> result;

    private List<Connector> connectors = new ArrayList<>();
    private List<Thread> connectorThreads = new ArrayList<>();
    private Processor processor;

    private InterruptibleInputReader in;

    Client(int id, Properties properties) {
        this.id = id;
        this.properties = properties;

        int amount = properties.getServerAmount();
        for (int i = 0; i < amount; i++) {
            outQueues.add(new LinkedBlockingQueue<>());
        }
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

    ResultParser getResultParser() {
        return resultParser;
    }

    BlockingQueue<Message> getIncomingMessageQueue() {
        return inQueue;
    }

    BlockingQueue<Message> getOutgoingMessageQueue(int id) {
        return outQueues.get(id);
    }

    Operation<?, ?> pollOperation() {
        Operation<?, ?> operation = this.operation;
        this.operation = null;
        return operation;
    }

    void setResult(Result<?> result) {
        this.result = result;
    }

    @Override
    public void run() {
        start();

        for (int i = 0; i < properties.getServerAmount(); i++) {
            Connector connector = new Connector(i, this);
            Thread connectorThread = new Thread(connector);
            connectorThread.start();
            connectors.add(connector);
            connectorThreads.add(connectorThread);
        }

        Processor processor = new Processor(inQueue, outQueues, this);
        Thread processorThread = new Thread(processor);
        processorThread.start();
        this.processor = processor;

        in = new InterruptibleInputReader(new FileInputStream(FileDescriptor.in).getChannel());
        while (isRunning()) {
            String line = in.readLine();
            if (line == null) {
                stop();
            } else {
                Operation<?, ?> operation = operationParser.parse(line);
                if (operation != null) {
                    result = null;
                    this.operation = operation;
                    while (isRunning() && result == null) {
                        try {
                            Thread.sleep(PROCESS_TIMEOUT);
                        } catch (InterruptedException ignored) {
                            stop();
                        }
                    }
                    System.out.println(result);
                    result = null;
                }
            }
        }

        try {
            for (Thread connectorThread : connectorThreads) {
                connectorThread.join();
            }
            processorThread.join();
        } catch (InterruptedException ignored) {
            System.err.println("OOPS!");
        }
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        in.close();

        connectors.forEach(SafeRunnable::stop);
        processor.stop();

        return true;
    }
}
