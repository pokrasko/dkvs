package ru.pokrasko.dkvs.waiting;

import ru.pokrasko.dkvs.messages.Message;

public class StateTransferUpdateWaiting extends StateTransferWaiting {
    private int lastNeeded;

    public StateTransferUpdateWaiting(Message message, int receiver, int lastNeeded, long timeout, long period) {
        super(message, receiver, timeout, period);
        this.lastNeeded = lastNeeded;
    }

    public void newNeeded(int lastNeeded) {
        if (lastNeeded > this.lastNeeded) {
            this.lastNeeded = lastNeeded;
        }
    }

    public boolean gotNeeded(int opNumber) {
        return opNumber >= lastNeeded;
    }
}
