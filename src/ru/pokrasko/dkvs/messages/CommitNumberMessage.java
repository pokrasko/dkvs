package ru.pokrasko.dkvs.messages;

public abstract class CommitNumberMessage extends ViewNumberMessage {
    int commitNumber;

    CommitNumberMessage(String type, int viewNumber, int commitNumber) {
        super(type, viewNumber);
        this.commitNumber = commitNumber;
    }

    public int getCommitNumber() {
        return commitNumber;
    }
}
