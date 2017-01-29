package ru.pokrasko.dkvs;

import java.net.InetSocketAddress;
import java.util.List;

public class Properties {
    private int serverAmount;
    private List<InetSocketAddress> serverAddresses;
    private int timeout;

    public Properties(List<InetSocketAddress> serverAddresses, int timeout) {
        this.serverAddresses = serverAddresses;
        this.serverAmount = serverAddresses.size();
        this.timeout = timeout;
    }

    InetSocketAddress getServerAddress(int i) {
        return serverAddresses.get(i);
    }

    int getServerAmount() {
        return serverAmount;
    }

    int getTimeout() {
        return timeout;
    }
}
