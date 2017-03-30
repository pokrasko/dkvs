package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.SafeRunnable;
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
    private Server server;
    private ServerSender serverSender;

    private InetSocketAddress thatAddress;
    private int thisId;
    private int thatId;

    private BlockingQueue<Message> queue;
    private int timeout;

    Connector(int thatId, Server server) {
        this.server = server;

        this.thisId = server.getId();
        this.thatId = thatId;
        this.thatAddress = server.getServerAddress(thatId);

        this.queue = server.getOutgoingServerMessageQueue(thatId);
        this.timeout = server.getTimeout();
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
                    Thread.sleep(Main.CONNECT_TIMEOUT);
                } catch (InterruptedException ignored) {
                    stop();
                }
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

        private ServerSender(Socket socket) throws IOException {
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
                server.setConnectedOut(thatId);
                queue.clear();

                while (isRunning() && !writer.checkError()) {
                    Message message = queue.poll(timeout / 2, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        System.out.println("Sending to server #" + (thatId + 1) + " message: " + message);
                        writer.println(message);
                    } else {
                        writer.println(new PingMessage());
                    }
                }
            } catch (InterruptedException ignored) {
                stop();
            } finally {
                server.resetConnectedOut(thatId);
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
}


