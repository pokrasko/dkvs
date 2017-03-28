package ru.pokrasko.dkvs.quorums;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BooleanQuorum implements Quorum<Boolean> {
    protected int quorumNumber;
    protected List<Boolean> confirmed;
    protected int confirmedNumber;

    protected BooleanQuorum(int quorumNumber, int capacity) {
        this.quorumNumber = quorumNumber;
        this.confirmed = new ArrayList<>(Collections.nCopies(capacity, false));
    }

    @Override
    public Boolean check(int replicaId, Boolean value) {
        if (!confirmed.set(replicaId, value)) {
            confirmedNumber++;
        }
        return check();
    }

    public boolean check(int replicaId) {
        return check(replicaId, true);
    }
}
