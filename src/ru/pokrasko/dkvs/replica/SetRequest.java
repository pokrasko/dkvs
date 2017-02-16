package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.service.Operation;

import java.util.Map;

public class SetRequest extends Request<Map.Entry<String, String>, Void> {
    SetRequest(Operation<Map.Entry<String, String>, Void> operation, int clientId, int requestNumber) {
        super(operation, clientId, requestNumber);
    }
}
