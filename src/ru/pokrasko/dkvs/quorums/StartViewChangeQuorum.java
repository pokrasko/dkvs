package ru.pokrasko.dkvs.quorums;

public class StartViewChangeQuorum extends BooleanQuorum {
    public StartViewChangeQuorum(int quorumNumber, int capacity) {
        super(quorumNumber, capacity);
    }

    @Override
    public Boolean check() {
        return confirmedNumber >= quorumNumber;
    }
}
