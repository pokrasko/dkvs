package ru.pokrasko.dkvs.service;

public abstract class Result<R> {
    R result;

    Result(R result) {
        this.result = result;
    }

    @Override
    public abstract String toString();
}
