package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketConnecter implements Runnable {
    private Server server;
    private Socket socket;

    private InetSocketAddress thatAddress;
    private int thatId;

    public SocketConnecter(Server server, int thatId) {
        this.server = server;
        this.thatId = thatId;
        this.thatAddress = server.getServerAddress(thatId);
    }

    @Override
    public void run() {
        PrintWriter writer;
        while (!Thread.interrupted()) {
            try {
                socket = new Socket(thatAddress.getAddress(), thatAddress.getPort());

                writer = new PrintWriter(socket.getOutputStream());
                writer.println(server.getId());
                server.setConnectedOut(thatId, true);

                while (!writer.checkError()) {
                    writer.println(server.getOutgoingMessage().toString());
                }
            } catch (IOException e) {
                System.out.println("Couldn't connect from server #" + server.getId() + " to server #" + (thatId + 1)
                        + " (" + e.getMessage() + ")");
            } finally {
                server.setConnectedOut(thatId, false);
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
}
