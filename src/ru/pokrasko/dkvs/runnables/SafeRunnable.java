package ru.pokrasko.dkvs.runnables;

abstract class SafeRunnable implements Runnable {
    private volatile boolean isRunning;

    void start() {
        isRunning = true;
    }

    boolean isRunning() {
        return isRunning;
    }

    public void stop() {
        isRunning = false;
    }
}
