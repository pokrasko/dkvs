package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.messages.DoViewChangeMessage;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Replica;
import ru.pokrasko.dkvs.replica.Request;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NewPrimaryQuorum {
    private Replica replica;
    private List<Boolean> confirmed;
    private int confirmedNumber;
    private int viewNumber;

    private Log log;
    private int lastNormalViewNumber;
    private int opNumber;
    private int commitNumber;

    NewPrimaryQuorum(Replica replica, int replicaNumber) {
        this.replica = replica;
        this.confirmed = new ArrayList<>(Collections.nCopies(replicaNumber, false));
    }

    boolean checkQuorum(int viewNumber, int id) {
        assert viewNumber >= this.viewNumber;
        if (viewNumber > this.viewNumber) {
            this.viewNumber = viewNumber;
            Collections.fill(this.confirmed, false);
            confirmedNumber = 0;
        }
        if (!confirmed.set(id, true)) {
            confirmedNumber++;
        }
        return confirmedNumber >= replica.getIncludingQuorumNumber();
    }

    boolean checkQuorum(DoViewChangeMessage message) {
        assert message.getViewNumber() >= this.viewNumber;
        if (message.getViewNumber() > this.viewNumber) {
            this.viewNumber = message.getViewNumber();
            Collections.fill(this.confirmed, false);
            confirmedNumber = 0;
            lastNormalViewNumber = 0;
            commitNumber = 0;
        }
        if (!confirmed.set(message.getReplicaId(), true)) {
            confirmedNumber++;
            if (message.getLastNormalViewNumber() > lastNormalViewNumber) {
                lastNormalViewNumber = message.getLastNormalViewNumber();
                log = message.getLog();
                opNumber = message.getOpNumber();
            }
            if (message.getCommitNumber() > commitNumber) {
                commitNumber = message.getCommitNumber();
            }
        }
        return confirmedNumber >= replica.getIncludingQuorumNumber();
    }

    Log getLog() {
        return log;
    }

    int getOpNumber() {
        return opNumber;
    }

    int getCommitNumber() {
        return commitNumber;
    }
}
