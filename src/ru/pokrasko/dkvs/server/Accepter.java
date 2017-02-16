package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.messages.Message;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

class Accepter extends SafeRunnable {
    private Server server;
    private ServerSocket serverSocket;

    private BlockingQueue<Message> incomingQueue;

    Accepter(Server server, ServerSocket serverSocket) {
        this.server = server;
        this.serverSocket = serverSocket;

        this.incomingQueue = server.getIncomingMessageQueue();
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
                Receiver receiver = new Receiver(socket, incomingQueue, server);
                Thread receiverThread = new Thread(receiver);
                receiverThread.start();
                server.registerReceiver(receiver, receiverThread);
            } catch (IOException e) {
                System.err.println("Couldn't run a socket reader (" + e.getMessage() + ")");
                stop();
            }
        }
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        try {
            serverSocket.close();
        } catch (IOException ignored) {}

        return true;
    }
}
