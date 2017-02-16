package ru.pokrasko.dkvs.messages;

public class PrepareOkMessage extends ViewedMessage {
    private int opNumber;
    private int replicaId;

    public PrepareOkMessage(int viewNumber, int opNumber, int replicaId) {
        super(viewNumber);
        this.opNumber = opNumber;
        this.replicaId = replicaId;
    }

    public int getOpNumber() {
        return opNumber;
    }

    public int getReplicaId() {
        return replicaId;
    }

    @Override
    public String toString() {
        return "PrepareOk " + viewNumber + " " + opNumber + " " + replicaId;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof PrepareOkMessage
                && opNumber == ((PrepareOkMessage) o).opNumber
                && replicaId == ((PrepareOkMessage) o).replicaId;
    }
}
