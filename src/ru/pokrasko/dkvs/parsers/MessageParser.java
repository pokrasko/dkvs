package ru.pokrasko.dkvs.parsers;

import ru.pokrasko.dkvs.messages.*;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Request;
import ru.pokrasko.dkvs.service.Operation;
import ru.pokrasko.dkvs.service.Result;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;

public class MessageParser extends LineParser {
    private ResultParser resultParser;

    public MessageParser() {
        this(new ResultParser());
    }

    public MessageParser(ResultParser resultParser) {
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

        Class<? extends Parsable> messageType = null;
        switch (type) {
            case "ping": {
                messageType = PingMessage.class;
                break;
            } case "node": {
                messageType = NewReplicaMessage.class;
                break;
            } case "ACCEPTED": {
                messageType = AcceptedMessage.class;
                break;
            } case "Request": {
                messageType = RequestMessage.class;
                break;
            } case "Prepare": {
                messageType = PrepareMessage.class;
                break;
            } case "Commit": {
                messageType = CommitMessage.class;
                break;
            } case "PrepareOk": {
                messageType = PrepareOkMessage.class;
                break;
            } case "Reply": {
                messageType = ReplyMessage.class;
                break;
            } case "StartViewChange": {
                messageType = StartViewChangeMessage.class;
                break;
            } case "DoViewChange": {
                messageType = DoViewChangeMessage.class;
                break;
            } case "StartView": {
                messageType = StartViewMessage.class;
                break;
            } case "Recovery": {
                messageType = RecoveryMessage.class;
                break;
            } case "RecoveryResponse": {
                messageType = RecoveryResponseMessage.class;
                break;
            } case "GetState": {
                messageType = GetStateMessage.class;
                break;
            } case "NewState": {
                messageType = NewStateMessage.class;
                break;
            }
        }

        if (messageType != null) {
            List<Parsable.Token> tokens;
            try {
                tokens = (List<Parsable.Token>) messageType.getMethod("tokens").invoke(null);
            } catch (Exception e) {
                System.err.println("Can't find tokens method for " + messageType.getName() + " class: "
                        + e.getMessage());
                return null;
            }

            List<Object> arguments = new LinkedList<>();
            for (Parsable.Token token : tokens) {
                try {
                    String word = null;
                    switch (token.type) {
                        case CHAR: {
                            readChar((Character) token.value);
                            break;
                        } case BYTE: {
                            arguments.add(parseByte());
                            break;
                        } case INTEGER: {
                            arguments.add(parseInteger());
                            break;
                        } case LONG: {
                            arguments.add(parseLong());
                            break;
                        } case PURE_WORD: {
                            word = parsePureWord();
                            break;
                        } case DIRTY_WORD: {
                            word = parseDirtyWord();
                            break;
                        } case WORD_TO_DELIMITER: {
                            word = parseWordToDelimiter((Character) token.value);
                            break;
                        } case OBJECT: {
                            Class<?> objectClass = (Class<?>) token.value;
                            if (objectClass == Log.class) {
                                readChar('[');
                                arguments.add(new Log.LogParser().parse(parseWordToDelimiter(']')));
                                readChar(']');
                            } else if (objectClass == Request.class) {
                                readChar('{');
                                arguments.add(new Request.RequestParser().parse(parseWordToDelimiter('}')));
                                readChar('}');
                            } else if (objectClass == Result.class) {
                                readChar('<');
                                arguments.add(resultParser.parse(parseWordToDelimiter('>')));
                                readChar('>');
                            } else {
                                throw new ParseException("invalid object type to read: " + objectClass.getName(),
                                        tokenBegin);
                            }
                            break;
                        }
                    }

                    if ((token.type == Parsable.Token.Type.PURE_WORD || token.type == Parsable.Token.Type.DIRTY_WORD
                            || token.type == Parsable.Token.Type.WORD_TO_DELIMITER)
                            && token.value != null && word != token.value) {
                        throw new ParseException("wrong word: expected \"" + token.value + "\", got \"" + word + "\"",
                                tokenBegin);
                    }

                    if (word != null) {
                        arguments.add(word);
                    }
                } catch (ParseException e) {
                    return logError("Invalid " + type + " message", line, e);
                }
            }

            try {
                checkEnd();
            } catch (ParseException e) {
                return logError("Invalid " + type + " message", line, e);
            }

            try {
                return (Message) messageType.getMethod("construct", Object[].class)
                        .invoke(null, (Object) arguments.toArray());
            } catch (Exception e) {
                System.err.println("Couldn't construct a message from line \"" + line + "\": " + e.getMessage());
                return null;
            }
        }

        switch (type) {
//            case "ping": {
//                try {
//                    checkEnd();
//                } catch (ParseException e) {
//                    return logError("Invalid ping message", line, e);
//                }
//
//                return new PingMessage();
//            }
//            case "node": {
//                int id;
//                try {
//                    id = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read new node id", line, e);
//                }
//
//                try {
//                    checkEnd();
//                } catch (ParseException e) {
//                    return logError("Invalid node message", line, e);
//                }
//
//                return new NewReplicaMessage(id);
//            }
//            case "ACCEPTED": {
//                try {
//                    checkEnd();
//                } catch (ParseException e) {
//                    return logError("Invalid accepted message", line, e);
//                }
//
//                return new AcceptedMessage();
//            }
//            case "Request": {
//                try {
//                    readChar('#');
//                } catch (ParseException e) {
//                    return logError("Invalid request message", line, e);
//                }
//
//                int clientId;
//                try {
//                    clientId = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read request's client id", line, e);
//                }
//
//                try {
//                    readChar('-');
//                } catch (ParseException e) {
//                    return logError("Invalid request message", line, e);
//                }
//
//                int requestNumber;
//                try {
//                    requestNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read request number", line, e);
//                }
//
//                try {
//                    readChar(':');
//                } catch (ParseException e) {
//                    return logError("Invalid request message", line, e);
//                }
//
//                Operation<?, ?> operation = operationParser.parse(line.substring(curIndex));
//                if (operation == null) {
//                    return null;
//                }
//
//                return new RequestMessage(Request.fromOperation(operation, clientId, requestNumber));
//            }
//            case "Prepare": {
//                int viewNumber;
//                try {
//                    viewNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read view number", line, e);
//                }
//
//                int opNumber;
//                try {
//                    opNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read operation number", line, e);
//                }
//
//                int commitNumber;
//                try {
//                    commitNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read commit number", line, e);
//                }
//                try {
//                    readChar('#');
//                } catch (ParseException e) {
//                    return logError("Invalid prepare message", line, e);
//                }
//
//                int clientId;
//                try {
//                    clientId = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read request's client id", line, e);
//                }
//
//                try {
//                    readChar('-');
//                } catch (ParseException e) {
//                    return logError("Invalid prepare message", line, e);
//                }
//
//                int requestNumber;
//                try {
//                    requestNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read request number", line, e);
//                }
//
//                try {
//                    readChar(':');
//                } catch (ParseException e) {
//                    return logError("Invalid prepare message", line, e);
//                }
//
//                Operation<?, ?> operation = operationParser.parse(line.substring(curIndex));
//                if (operation == null) {
//                    return null;
//                }
//
//                return new PrepareMessage(viewNumber, Request.fromOperation(operation, clientId, requestNumber),
//                        opNumber, commitNumber);
//            }
//            case "Commit": {
//                int viewNumber;
//                try {
//                    viewNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read view number", line, e);
//                }
//
//                int commitNumber;
//                try {
//                    commitNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read commit number", line, e);
//                }
//
//                try {
//                    checkEnd();
//                } catch (ParseException e) {
//                    return logError("Invalid commit message", line, e);
//                }
//
//                return new CommitMessage(viewNumber, commitNumber);
//            }
//            case "PrepareOk": {
//                int viewNumber;
//                try {
//                    viewNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read view number", line, e);
//                }
//
//                int opNumber;
//                try {
//                    opNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read operation number", line, e);
//                }
//
//                int replicaId;
//                try {
//                    replicaId = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read replica ID", line, e);
//                }
//
//                try {
//                    checkEnd();
//                } catch (ParseException e) {
//                    return logError("Invalid commit message", line, e);
//                }
//
//                return new PrepareOkMessage(viewNumber, opNumber, replicaId);
//            }
//            case "Reply": {
//                int viewNumber;
//                try {
//                    viewNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read view number", line, e);
//                }
//
//                int requestNumber;
//                try {
//                    requestNumber = parseInteger();
//                } catch (ParseException e) {
//                    return logError("Couldn't read request number", line, e);
//                }
//
//                try {
//                    readChar(':');
//                } catch (ParseException e) {
//                    return logError("Invalid reply message", line, e);
//                }
//
//                Result<?> result = resultParser.parse(line.substring(curIndex));
//                if (result == null) {
//                    return null;
//                }
//
//                return new ReplyMessage(viewNumber, requestNumber, result);
//            }
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
