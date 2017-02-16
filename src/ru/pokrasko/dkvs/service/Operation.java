package ru.pokrasko.dkvs.service;

public abstract class Operation<A, R> {
    private String name;
    A argument;

    Operation(String name, A argument) {
        this.name = name;
        this.argument = argument;
    }

    A getArgument() {
        return argument;
    }

    @Override
    public String toString() {
        return name + " " + argument;
    }

    public abstract Result<R> initResult(R result);
}
