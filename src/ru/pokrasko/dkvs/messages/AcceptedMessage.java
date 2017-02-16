package ru.pokrasko.dkvs.messages;

public class AcceptedMessage extends Message {
    private int id;

    public AcceptedMessage() {}

    public AcceptedMessage(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "ACCEPTED";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof AcceptedMessage
                && id == ((AcceptedMessage) o).id;
    }
}
