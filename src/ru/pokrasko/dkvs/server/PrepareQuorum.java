package ru.pokrasko.dkvs.server;

import ru.pokrasko.dkvs.replica.Replica;

import java.util.*;

class PrepareQuorum {
    private Replica replica;
    private Map<Integer, Integer> confirmed = new HashMap<>();
    private Map<Integer, Integer> confirmedNumber = new HashMap<>();

    PrepareQuorum(Replica replica) {
        this.replica = replica;
    }

    int checkQuorum(int opNumber, int id) {
        Integer previousOpNumber = confirmed.put(id, opNumber);
        if (previousOpNumber == null) {
            previousOpNumber = 0;
        }
        int result = previousOpNumber;
        while (previousOpNumber < opNumber) {
            Integer number = confirmedNumber.get(++previousOpNumber);
            if (number == null) {
                number = 0;
            }
            confirmedNumber.put(previousOpNumber, number + 1);
            if (number + 1 >= replica.getExcludingQuorumNumber() && result == previousOpNumber - 1) {
                result = previousOpNumber;
            }
        }
        return result;
    }
}
