package ru.pokrasko.dkvs.parsers;

import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.replica.Request;
import ru.pokrasko.dkvs.service.Operation;
import ru.pokrasko.dkvs.service.Result;

import java.text.ParseException;

public class MessageParser extends LineParser {
    private OperationParser operationParser;
    private ResultParser resultParser;

    public MessageParser() {
        this(new ResultParser());
    }

    public MessageParser(ResultParser resultParser) {
        operationParser = new OperationParser();
        this.resultParser = resultParser;
    }

    @Override
    public Message parse(String line) {
        init(line);

        String type;
        try {
            type = parsePureWord();
        } catch (ParseException e) {
            return logError("Couldn't read message type", line, e);
        }

        switch (type) {
            case "ping": {
                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid ping message", line, e);
                }

                return new PingMessage();
            }
            case "node": {
                int id;
                try {
                    id = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read new node id", line, e);
                }

                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid node message", line, e);
                }

                return new NewReplicaMessage(id);
            }
            case "ACCEPTED": {
                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid accepted message", line, e);
                }

                return new AcceptedMessage();
            }
            case "Request": {
                try {
                    readChar('#');
                } catch (ParseException e) {
                    return logError("Invalid request message", line, e);
                }

                int clientId;
                try {
                    clientId = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read request's client id", line, e);
                }

                try {
                    readChar('-');
                } catch (ParseException e) {
                    return logError("Invalid request message", line, e);
                }

                int requestNumber;
                try {
                    requestNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read request number", line, e);
                }

                try {
                    readChar(':');
                } catch (ParseException e) {
                    return logError("Invalid request message", line, e);
                }

                Operation<?, ?> operation = operationParser.parse(line.substring(curIndex));
                if (operation == null) {
                    return null;
                }

                return new RequestMessage(Request.fromOperation(operation, clientId, requestNumber));
            }
            case "Prepare": {
                int viewNumber;
                try {
                    viewNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read view number", line, e);
                }

                int opNumber;
                try {
                    opNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read operation number", line, e);
                }

                int commitNumber;
                try {
                    commitNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read commit number", line, e);
                }
                try {
                    readChar('#');
                } catch (ParseException e) {
                    return logError("Invalid prepare message", line, e);
                }

                int clientId;
                try {
                    clientId = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read request's client id", line, e);
                }

                try {
                    readChar('-');
                } catch (ParseException e) {
                    return logError("Invalid prepare message", line, e);
                }

                int requestNumber;
                try {
                    requestNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read request number", line, e);
                }

                try {
                    readChar(':');
                } catch (ParseException e) {
                    return logError("Invalid prepare message", line, e);
                }

                Operation<?, ?> operation = operationParser.parse(line.substring(curIndex));
                if (operation == null) {
                    return null;
                }

                return new PrepareMessage(viewNumber, opNumber, commitNumber,
                        Request.fromOperation(operation, clientId, requestNumber));
            }
            case "Commit": {
                int viewNumber;
                try {
                    viewNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read view number", line, e);
                }

                int commitNumber;
                try {
                    commitNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read commit number", line, e);
                }

                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid commit message", line, e);
                }

                return new CommitMessage(viewNumber, commitNumber);
            }
            case "PrepareOk": {
                int viewNumber;
                try {
                    viewNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read view number", line, e);
                }

                int opNumber;
                try {
                    opNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read operation number", line, e);
                }

                int replicaId;
                try {
                    replicaId = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read replica ID", line, e);
                }

                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid commit message", line, e);
                }

                return new PrepareOkMessage(viewNumber, opNumber, replicaId);
            }
            case "Reply": {
                int viewNumber;
                try {
                    viewNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read view number", line, e);
                }

                int requestNumber;
                try {
                    requestNumber = parseInteger();
                } catch (ParseException e) {
                    return logError("Couldn't read request number", line, e);
                }

                try {
                    readChar(':');
                } catch (ParseException e) {
                    return logError("Invalid reply message", line, e);
                }

                Result<?> result = resultParser.parse(line.substring(curIndex));
                if (result == null) {
                    return null;
                }

                return new ReplyMessage(viewNumber, requestNumber, result);
            }
            default: {
                return logError("Invalid message type \"" + type + "\"", line);
            }
        }
    }

    @Override
    protected Message logError(String message, String line, Exception e) {
        return (Message) super.logError(message, line, e);
    }

    @Override
    protected Message logError(String message, String line) {
        return (Message) super.logError(message, line);
    }
}
