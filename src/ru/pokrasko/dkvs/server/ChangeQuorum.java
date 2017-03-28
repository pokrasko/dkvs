package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.replica.Replica;

import java.util.*;

public class ChangeQuorum {
    private Replica replica;
    private List<Boolean> confirmed;
    private int confirmedNumber;
    private int viewNumber;

    ChangeQuorum(Replica replica, int replicaNumber) {
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
        return confirmedNumber >= replica.getExcludingQuorumNumber();
    }
}
