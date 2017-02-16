package ru.pokrasko.dkvs.messages;

public class NewReplicaMessage extends Message {
    private int id;

    public NewReplicaMessage(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "node " + id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof NewReplicaMessage
                && id == ((NewReplicaMessage) o).id;
    }
}
