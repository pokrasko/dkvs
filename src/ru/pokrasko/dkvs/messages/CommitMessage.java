package ru.pokrasko.dkvs.messages;

public class CommitMessage extends ViewedMessage {
    private int commitNumber;

    public CommitMessage(int viewNumber, int commitNumber) {
        super(viewNumber);
        this.commitNumber = commitNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }

    @Override
    public String toString() {
        return "Commit " + viewNumber + " " + commitNumber;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CommitMessage
                && commitNumber == ((CommitMessage) o).commitNumber;
    }
}
