package ru.pokrasko.dkvs.client;

import ru.pokrasko.dkvs.SafeRunnable;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PingMessage;
import ru.pokrasko.dkvs.parsers.MessageParser;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

class Connector extends SafeRunnable {
    private Client client;

    private Sender sender;
    private Receiver receiver;

    private InetSocketAddress thatAddress;
    private int thisId;
    private int thatId;

    private BlockingQueue<Message> incomingQueue;
    private BlockingQueue<Message> outgoingQueue;
    private int timeout;

    private Socket socket;

    Connector(int thatId, Client client) {
        this.client = client;

        thisId = client.getId();
        this.thatId = thatId;
        thatAddress = client.getServerAddress(thatId);

        incomingQueue = client.getIncomingMessageQueue();
        outgoingQueue = client.getOutgoingMessageQueue(thatId);
        timeout = client.getTimeout();
    }

    @Override
    public void run() {
        start();

        while (isRunning()) {
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
                socket.setSoTimeout(timeout);

                sender = new Sender();
                Thread senderThread = new Thread(sender);
                senderThread.start();

                receiver = new Receiver();
                Thread receiverThread = new Thread(receiver);
                receiverThread.start();

                try {
                    senderThread.join();
                    receiverThread.join();
                    System.out.println("Closing connection to server #" + (thatId + 1));
                    sender = null;
                    receiver = null;
                } catch (InterruptedException ignored) {
                    stop();
                }
            } catch (IOException ignored) {
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }

    @Override
    public boolean stop() {
        if (!super.stop()) {
            return false;
        }

        if (sender != null) {
            sender.stop();
        }

        if (receiver != null) {
            receiver.stop();
        }

        return true;
    }


    private class Sender extends SafeRunnable {
        private PrintWriter writer;

        private Sender() throws IOException {
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
                writer.println("Client " + thisId);

                while (isRunning() && !writer.checkError()) {
                    Message message = outgoingQueue.poll(timeout / 2, TimeUnit.MILLISECONDS);
                    if (message != null) {
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


    private class Receiver extends SafeRunnable {
        private BufferedReader reader;

        private Receiver() throws IOException {
            try {
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
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

            while (isRunning()) {
                try {
                    String line = reader.readLine();
                    if (line == null) {
                        stop();
                    } else {
                        Message message = new MessageParser(client.getResultParser()).parse(line);
                        if (message != null && !(message instanceof PingMessage)) {
                            incomingQueue.put(message);
                        }
                    }
                } catch (InterruptedException | IOException ignored) {
                    stop();
                }
            }
        }
    }
}
