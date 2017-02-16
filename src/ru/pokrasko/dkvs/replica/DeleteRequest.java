package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.service.Operation;

public class DeleteRequest extends Request<String, Boolean> {
    DeleteRequest(Operation<String, Boolean> operation, int clientId, int requestNumber) {
        super(operation, clientId, requestNumber);
    }
}
