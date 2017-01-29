package ru.pokrasko.dkvs;

import java.net.InetSocketAddress;
import java.util.List;

class Properties {
    private List<InetSocketAddress> serverAddresses;
    private int timeout;

    Properties(List<InetSocketAddress> serverAddresses, int timeout) {
        this.serverAddresses = serverAddresses;
        this.timeout = timeout;
    }

    InetSocketAddress getServerAddress(int i) {
        return serverAddresses.get(i);
    }

    int getTimeout() {
        return timeout;
    }
}
