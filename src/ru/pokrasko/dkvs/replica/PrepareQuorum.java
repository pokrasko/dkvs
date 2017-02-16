package ru.pokrasko.dkvs.replica;

import java.util.*;

class PrepareQuorum {
    private Replica replica;
    private int replicaNumber;
    private Map<Integer, List<Boolean>> confirmed = new HashMap<>();
    private Map<Integer, Integer> confirmedNumber = new HashMap<>();

    PrepareQuorum(Replica replica, int replicaNumber) {
        this.replica = replica;
        this.replicaNumber = replicaNumber;
    }

    void initQuorum(int opNumber) {
        confirmed.put(opNumber, new ArrayList<>(Collections.nCopies(replicaNumber, false)));
        confirmedNumber.put(opNumber, 0);
    }

    boolean checkQuorum(int opNumber, int id) {
        if (confirmedNumber.get(opNumber) == replica.getExcludingQuorumNumber() || confirmed.get(opNumber).get(id)) {
            return false;
        }
        confirmed.get(opNumber).set(id, true);
        int number = confirmedNumber.get(opNumber);
        confirmedNumber.put(opNumber, number + 1);
        return number + 1 == replica.getExcludingQuorumNumber();
    }
}
