package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.service.Operation;

public class GetRequest extends Request<String, String> {
    GetRequest(Operation<String, String> operation, int clientId, int requestNumber) {
        super(operation, clientId, requestNumber);
    }
}
