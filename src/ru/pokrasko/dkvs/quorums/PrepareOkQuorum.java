package ru.pokrasko.dkvs.quorums;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

public class PrepareOkQuorum implements Quorum<Integer> {
    private int quorumNumber;
    private List<Integer> confirmed;
    private List<Integer> confirmedNumber;
    private int lastConfirmed;

    public PrepareOkQuorum(int quorumNumber, int capacity) {
        this.quorumNumber = quorumNumber;
        if (quorumNumber != 1) {
            this.confirmed = new ArrayList<>(Collections.nCopies(capacity, 0));
            this.confirmedNumber = new ArrayList<>();
        }
    }

    @Override
    public Integer check() {
        return lastConfirmed;
    }

    @Override
    public Integer check(int replicaId, Integer value) {
        if (quorumNumber == 1) {
            lastConfirmed = value;
            return value;
        }

        int thisLastConfirmed = confirmed.set(replicaId, value);
        int i = Math.max(thisLastConfirmed, lastConfirmed) - lastConfirmed;
        int relativeValue = value - lastConfirmed;
        int oldLastConfirmed = lastConfirmed;

        for (i++; i <= Math.min(relativeValue, lastConfirmed); i++) {
            int thisConfirmedNumber = confirmedNumber.get(i - 1) + 1;
            if (thisConfirmedNumber == quorumNumber) {
                lastConfirmed = oldLastConfirmed + i;
            } else {
                confirmedNumber.set(i - 1, thisConfirmedNumber);
            }
        }
        for (i = confirmedNumber.size(); i < relativeValue; i++) {
            confirmedNumber.add(1);
        }

        ListIterator<Integer> iterator = confirmedNumber.listIterator();
        for (i = oldLastConfirmed; i < lastConfirmed - oldLastConfirmed; i++) {
            iterator.next();
            iterator.remove();
        }

        return lastConfirmed != oldLastConfirmed ? lastConfirmed : -lastConfirmed;
    }
}
