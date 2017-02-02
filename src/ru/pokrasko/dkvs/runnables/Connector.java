package ru.pokrasko.dkvs.runnables;

import ru.pokrasko.dkvs.Main;
import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.Server;
import ru.pokrasko.dkvs.messages.PingMessage;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Connector extends SafeRunnable {
    private List<AtomicBoolean> isConnectedOut;
    private BlockingQueue<Message> queue;
    private int timeout;

    private InetSocketAddress thatAddress;
    private int thisId;
    private int thatId;

    public Connector(Server server, int thatId) {
        this.isConnectedOut = server.getIsConnectedOut();
        timeout = server.getTimeout();

        thisId = server.getId();
        this.thatId = thatId;
        thatAddress = server.getServerAddress(thatId);

        queue = server.getOutgoingMessageQueue(thisId);
    }

    @Override
    public void run() {
        start();

        while (isRunning()) {
            Socket socket;
            try {
                socket = new Socket(thatAddress.getAddress(), thatAddress.getPort());
                System.out.println("Established connection to #" + (thatId + 1));
            } catch (IOException e) {
                try {
                    Thread.sleep(Main.ACCEPT_TIMEOUT);
                } catch (InterruptedException ignored) {}
                continue;
            }

            try {
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
                writer.println(thisId);
                isConnectedOut.get(thatId).set(true);

                while (isRunning() && !writer.checkError()) {
                    Message message = queue.poll(timeout / 2, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        System.out.println("Sending message: " + message);
                        writer.println(message);
                    } else {
                        writer.println(new PingMessage());
                    }
                }
            } catch (InterruptedException ignored) {
            } catch (IOException e) {
                System.err.println("Couldn't connect to server #" + (thatId + 1)
                        + " (" + e.getMessage() + ")");
            } finally {
                System.out.println("Closing outgoing connection to #" + (thatId + 1));
                isConnectedOut.get(thatId).set(false);
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
}
