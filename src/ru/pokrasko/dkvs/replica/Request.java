package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.parsers.LineParser;
import ru.pokrasko.dkvs.parsers.OperationParser;
import ru.pokrasko.dkvs.service.*;

import java.text.ParseException;

public class Request<A, R> {
    private Operation<A, R> operation;
    private int clientId;
    private int requestNumber;
    private Result<R> result;

    public Request(Operation<A, R> operation, int clientId, int requestNumber) {
        this.operation = operation;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
    }

    public Operation<A, R> getOperation() {
        return operation;
    }

    public int getClientId() {
        return clientId;
    }

    public int getRequestNumber() {
        return requestNumber;
    }

    public Result<R> getResult() {
        return result;
    }

    public void setResult(Result<R> result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "{" + operation + " #" + clientId + "-" + requestNumber + "}";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Request<?, ?>
                && clientId == ((Request<?, ?>) o).clientId
                && requestNumber == ((Request<?, ?>) o).requestNumber;
    }

    public static Request<?, ?> fromOperation(Operation<?, ?> operation, int clientId, int requestNumber) {
        if (operation instanceof GetOperation) {
            return new GetRequest((GetOperation) operation, clientId, requestNumber);
        } else if (operation instanceof SetOperation) {
            return new SetRequest((SetOperation) operation, clientId, requestNumber);
        } else if (operation instanceof DeleteOperation) {
            return new DeleteRequest((DeleteOperation) operation, clientId, requestNumber);
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static class RequestParser extends LineParser {
        private OperationParser operationParser;

        public RequestParser() {
            this.operationParser = new OperationParser();
        }

        @Override
        public Request<?, ?> parse(String line) {
            init(line);

            try {
                readChar('<');
                String operationLine = parseWordToDelimiter('>');
                Operation<?, ?> operation = operationParser.parse(operationLine);
                if (operation == null) {
                    return null;
                }
                readChar('>');

                readChar('#');
                int clientId = parseInteger();
                readChar('-');
                int requestNumber = parseInteger();

                checkEnd();
                return Request.fromOperation(operation, clientId, requestNumber);
            } catch (ParseException e) {
                return (Request<?, ?>) logError("Couldn't read a request", line, e);
            }
        }
    }
}
