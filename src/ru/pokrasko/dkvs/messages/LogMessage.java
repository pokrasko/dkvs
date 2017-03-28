package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.replica.Log;

public abstract class LogMessage extends CommitNumberMessage {
    Log log;
    int opNumber;

    LogMessage(String type, int viewNumber, Log log, int opNumber, int commitNumber) {
        super(type, viewNumber, commitNumber);
        this.log = log;
        this.opNumber = opNumber;
    }

    public Log getLog() {
        return log;
    }

    public int getOpNumber() {
        return opNumber;
    }
}
