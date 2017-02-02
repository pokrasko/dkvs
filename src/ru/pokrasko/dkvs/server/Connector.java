package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.Main;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PingMessage;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class Connector extends SafeRunnable {
    private ServerSender serverSender;

    private BlockingQueue<Message> queue;
    private int timeout;

    private InetSocketAddress thatAddress;
    private int thisId;
    private int thatId;

    Connector(int thatId, Server server) {
        timeout = server.getTimeout();

        thisId = server.getId();
        this.thatId = thatId;
        thatAddress = server.getServerAddress(thatId);

        queue = server.getOutgoingServerMessageQueue(thisId);
    }

    @Override
    public void run() {
        start();

        while (isRunning()) {
            Socket socket;
            try {
                socket = new Socket(thatAddress.getAddress(), thatAddress.getPort());
                System.out.println("Established connection to server #" + (thatId + 1));
            } catch (IOException e) {
                try {
                    Thread.sleep(Main.ACCEPT_TIMEOUT);
                } catch (InterruptedException ignored) {}
                continue;
            }

            try {
                serverSender = new ServerSender(socket);
                serverSender.run();
                System.out.println("Closing outgoing connection to server #" + (thatId + 1));
            } catch (IOException ignored) {}
        }
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        if (serverSender != null) {
            serverSender.stop();
        }

        return true;
    }


    private class ServerSender extends SafeRunnable {
        private Socket socket;
        private PrintWriter writer;

        ServerSender(Socket socket) throws IOException {
            this.socket = socket;
            try {
                writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            } catch (IOException e) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
                throw e;
            }
        }

        @Override
        public void run() {
            start();

            try {
                writer.println("Server " + thisId);

                while (isRunning() && !writer.checkError()) {
                    Message message = queue.poll(timeout / 2, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        System.out.println("Sending to server #" + thatId + " message: " + message);
                        writer.println(message);
                    } else {
                        writer.println(new PingMessage());
                    }
                }
            } catch (InterruptedException ignored) {
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
}


