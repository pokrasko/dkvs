package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.messages.AcceptedMessage;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PingMessage;
import ru.pokrasko.dkvs.parsers.MessageParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

class Receiver extends SafeRunnable {
    private Server server;

    private Socket socket;
    private BufferedReader reader;

    private boolean isThatServer;
    private int thatId;

    private BlockingQueue<Message> queue;

    private MessageParser messageParser;

    Receiver(Socket socket, BlockingQueue<Message> queue, Server server)
            throws IOException {
        this.socket = socket;
        this.socket.setSoTimeout(server.getTimeout());
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        this.queue = queue;

        this.messageParser = new MessageParser();

        this.server = server;
    }

    @Override
    public void run() {
        start();

        try {
            String line = reader.readLine();
            if (!line.startsWith("Server") && !line.startsWith("Client")) {
                return;
            }

            isThatServer = line.startsWith("Server");
            thatId = Integer.parseInt(line.substring(6).trim());
            System.out.println("Received connection from " + serverOrClient() + " #" + (thatId + 1));

            if (line.startsWith("Client")) {
                ClientSender sender = new ClientSender(socket, thatId, server);
                Thread senderThread = new Thread(sender);
                senderThread.start();
                server.registerSender(sender, senderThread);
            } else {
                server.setConnectedIn(thatId);
            }

            while (isRunning()) {
                try {
                    line = reader.readLine();
                    if (line == null) {
                        stop();
                    } else {
                        Message message = messageParser.parse(line);
                        if (message == null) {
                            System.err.println("Wrong message received from " + serverOrClient() + " #" + (thatId + 1)
                                    + ": " + line);
                        } else if (!(message instanceof PingMessage)) {
                            System.out.println("Received message from " + serverOrClient() + " #" + (thatId + 1)
                                        + ": " + message);
                            if (message instanceof AcceptedMessage) {
                                ((AcceptedMessage) message).setId(thatId);
                            }
                            queue.put(message);
                        }
                    }
                } catch (InterruptedException | IOException ignored) {
                    stop();
                }
            }

            System.out.println("Closing incoming connection from " + serverOrClient() + " #" + (thatId + 1));
        } catch (IOException e) {
            System.err.println("Couldn't handshake on connection from " + socket.getRemoteSocketAddress()
                    + " (" + e.getMessage() + ")");
        } finally {
            if (server.isRunning()) {
                server.unregisterReceiver(this, Thread.currentThread());
            }
        }
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        if (isThatServer) {
            server.resetConnectedIn(thatId);
        }
        try {
            socket.close();
        } catch (IOException ignored) {}

        return true;
    }

    private String serverOrClient() {
        return isThatServer ? "server" : "client";
    }
}
