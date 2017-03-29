package ru.pokrasko.dkvs.quorums;

import ru.pokrasko.dkvs.replica.Replica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AcceptedQuorum {
    private Replica replica;
    private List<Boolean> confirmed;
    private int confirmedNumber;
    private int fullNumber;

    public AcceptedQuorum(Replica replica, int replicaNumber) {
        this.replica = replica;
        this.confirmed = new ArrayList<>(Collections.nCopies(replicaNumber, false));
        this.fullNumber = replicaNumber - 1;
    }

    public List<Boolean> getConfirmed() {
        return confirmed;
    }

    public boolean isConnected(int id) {
        return confirmed.get(id);
    }

    public boolean check() {
        return confirmedNumber >= replica.getExcludingQuorumNumber();
    }

    public boolean checkFull() {
        return confirmedNumber == fullNumber;
    }

    public boolean connect(int id) {
        if (confirmed.get(id)) {
            return false;
        }
        confirmed.set(id, true);
        return ++confirmedNumber == replica.getExcludingQuorumNumber();
    }

    public boolean disconnect(int id) {
        if (!confirmed.get(id)) {
            return false;
        }
        confirmed.set(id, false);
        return confirmedNumber-- == replica.getExcludingQuorumNumber();
    }
}
