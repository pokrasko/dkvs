package ru.pokrasko.dkvs.replica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class AcceptedQuorum {
    private Replica replica;
    private List<Boolean> confirmed;
    private int confirmedNumber;

    AcceptedQuorum(Replica replica, int replicaNumber) {
        this.replica = replica;
        confirmed = new ArrayList<>(Collections.nCopies(replicaNumber, false));
    }

    boolean connect(int id) {
        if (confirmed.get(id)) {
            return false;
        }
        confirmed.set(id, true);
        return ++confirmedNumber == replica.getExcludingQuorumNumber();
    }

    boolean disconnect(int id) {
        if (!confirmed.get(id)) {
            return false;
        }
        confirmed.set(id, false);
        return confirmedNumber-- == replica.getExcludingQuorumNumber();
    }
}
