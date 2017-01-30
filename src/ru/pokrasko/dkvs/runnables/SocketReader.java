package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Server;
import ru.pokrasko.dkvs.parsers.MessageParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

class SocketReader implements Runnable {
    private Server server;
    private Socket socket;
    private BufferedReader reader;

    private int id;
    private MessageParser messageParser;

    SocketReader(Server server, Socket socket) throws IOException {
        this.server = server;
        this.socket = socket;
        this.socket.setSoTimeout(server.getTimeout());
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.messageParser = new MessageParser();
    }

    @Override
    public void run() {
        try {
            id = Integer.parseInt(reader.readLine()) - 1;
        } catch (IOException e) {
            System.err.println("Couldn't read server id (" + e.getMessage() + ")");
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
        server.setConnectedIn(id, true);

        while (true) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    System.out.println("Socket from server #" + (id + 1) + " is closed");
                    return;
                }
                server.putIncomingMessage(messageParser.parse(line));
            } catch (IOException e) {
                System.err.println("Couldn't read a line (" + e.getMessage() + ")");
                return;
            } finally {
                server.setConnectedIn(id, false);
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
}
