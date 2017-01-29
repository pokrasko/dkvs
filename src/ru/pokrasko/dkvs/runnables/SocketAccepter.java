package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketAccepter implements Runnable {
    private Server server;
    private ServerSocket serverSocket;

    public SocketAccepter(Server server, ServerSocket serverSocket) {
        this.server = server;
        this.serverSocket = serverSocket;
    }

    @Override
    public void run() {
        Socket socket;
        while (!Thread.interrupted()) {
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                System.err.println("IO exception happened (" + e.getMessage() + ")");
                try {
                    Thread.sleep(100);
                    continue;
                } catch (InterruptedException e2) {
                    return;
                }
            }

            try {
                new Thread(new SocketReader(server, socket)).run();
            } catch (IOException e) {
                System.err.println("Couldn't run a socket reader (" + e.getMessage() + ")");
            }
        }
    }
}
