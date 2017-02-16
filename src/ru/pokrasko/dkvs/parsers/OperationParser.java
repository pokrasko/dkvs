package ru.pokrasko.dkvs.parsers;

import ru.pokrasko.dkvs.service.DeleteOperation;
import ru.pokrasko.dkvs.service.GetOperation;
import ru.pokrasko.dkvs.service.Operation;
import ru.pokrasko.dkvs.service.SetOperation;

import java.text.ParseException;

public class OperationParser extends LineParser {
    @Override
    public Operation<?, ?> parse(String line) {
        init(line);

        String type;
        try {
            type = parsePureWord();
        } catch (ParseException e) {
            return logError("Couldn't read operation type", line, e);
        }

        switch (type) {
            case "get": {
                String key;
                try {
                    key = parseDirtyWord();
                } catch (ParseException e) {
                    return logError("Couldn't read key", line, e);
                }

                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid get request", line, e);
                }

                return new GetOperation(key);
            } case "set": {
                String key;
                try {
                    key = parseDirtyWord();
                } catch (ParseException e) {
                    return logError("Couldn't read key", line, e);
                }

                String value = line.substring(curIndex).trim();

                return new SetOperation(key, value);
            } case "delete": {
                String key;
                try {
                    key = parseDirtyWord();
                } catch (ParseException e) {
                    return logError("Couldn't read key", line, e);
                }

                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid delete request", line, e);
                }

                return new DeleteOperation(key);
            } default: {
                return logError("Invalid request type \"" + type + "\"", line);
            }
        }
    }

    @Override
    protected Operation<?, ?> logError(String message, String line, Exception e) {
        return (Operation<?, ?>) super.logError(message, line, e);
    }

    @Override
    protected Operation<?, ?> logError(String message, String line) {
        return (Operation<?, ?>) super.logError(message, line);
    }
}
