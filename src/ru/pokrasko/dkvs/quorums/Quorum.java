package ru.pokrasko.dkvs.quorums;

public interface Quorum<T> {
    T check();
    T check(int replicaId, T value);
}
