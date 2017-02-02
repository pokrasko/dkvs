package ru.pokrasko.dkvs.server;

import java.util.concurrent.atomic.AtomicBoolean;

abstract class SafeRunnable implements Runnable {
    private volatile AtomicBoolean isRunning = new AtomicBoolean();

    void start() {
        isRunning.set(true);
    }

    boolean isRunning() {
        return isRunning.get();
    }

    public boolean stop() {
        return isRunning.compareAndSet(true, false);
    }
}
