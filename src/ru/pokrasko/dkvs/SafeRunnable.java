package ru.pokrasko.dkvs;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SafeRunnable implements Runnable {
    private volatile AtomicBoolean isRunning = new AtomicBoolean();

    protected void start() {
        isRunning.set(true);
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public boolean stop() {
        return isRunning.compareAndSet(true, false);
    }
}
