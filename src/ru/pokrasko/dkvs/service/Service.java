package ru.pokrasko.dkvs.service;

import ru.pokrasko.dkvs.replica.DeleteRequest;
import ru.pokrasko.dkvs.replica.GetRequest;
import ru.pokrasko.dkvs.replica.Request;
import ru.pokrasko.dkvs.replica.SetRequest;

import java.util.HashMap;
import java.util.Map;

public class Service {
    private Map<String, String> map = new HashMap<>();

    public <A, R> void commit(Request<A, R> request) {
        if (request instanceof GetRequest) {
            GetRequest getRequest = (GetRequest) request;
            Operation<String, String> getOperation = getRequest.getOperation();
            getRequest.setResult(getOperation.initResult(map.get(getOperation.getArgument())));
        } else if (request instanceof SetRequest) {
            SetRequest setRequest = (SetRequest) request;
            Operation<Map.Entry<String, String>, Void> setOperation = setRequest.getOperation();
            map.put(setOperation.getArgument().getKey(), setOperation.getArgument().getValue());
            setRequest.setResult(setOperation.initResult(null));
        } else if (request instanceof DeleteRequest) {
            DeleteRequest deleteRequest = (DeleteRequest) request;
            Operation<String, Boolean> deleteOperation = deleteRequest.getOperation();
            deleteRequest.setResult(deleteOperation.initResult(map.remove(deleteOperation.getArgument()) != null));
        } else {
            throw new IllegalArgumentException();
        }

        System.err.println("Commited request " + request + ", map = " + map);
    }
}
