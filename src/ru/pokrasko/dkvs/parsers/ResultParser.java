package ru.pokrasko.dkvs.parsers;

import ru.pokrasko.dkvs.service.*;

import java.text.ParseException;

public class ResultParser extends LineParser {
    private Class<? extends Result<?>> resultClass;

    @Override
    public synchronized Result<?> parse(String line) {
        init(line);

        String firstWord;
        try {
            firstWord = parsePureWord();
        } catch (ParseException e) {
            return logError("Couldn't read result", line, e);
        }

        switch (firstWord) {
            case "VALUE": {
                String key;
                try {
                    key = parseDirtyWord();
                } catch (ParseException e) {
                    return logError("Couldn't read result's key", line, e);
                }

                String value = line.substring(curIndex).trim();

                return new GetOperation(key).initResult(value);
            } case "NOT_FOUND": {
                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid NOT_FOUND result", line, e);
                }

                if (resultClass.isAssignableFrom(GetOperation.GetResult.class)) {
                    return new GetOperation(null).initResult(null);
                } else if (resultClass.isAssignableFrom(DeleteOperation.DeleteResult.class)) {
                    return new DeleteOperation.DeleteResult(false);
                } else {
                    System.err.println("Got NOT_FOUND result, but the last request should return a result of class "
                            + resultClass.getName());
                    return null;
                }
            } case "STORED": {
                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid STORED result", line, e);
                }

                return new SetOperation.SetResult(null);
            } case "DELETED": {
                try {
                    checkEnd();
                } catch (ParseException e) {
                    return logError("Invalid DELETED result", line, e);
                }

                return new DeleteOperation.DeleteResult(true);
            } default: {
                return logError("Invalid result first word \"" + firstWord + "\"", line);
            }
        }
    }

    public synchronized void setLastOperation(Operation<?, ?> operation) {
        if (operation instanceof GetOperation) {
            resultClass = GetOperation.GetResult.class;
        } else if (operation instanceof SetOperation) {
            resultClass = SetOperation.SetResult.class;
        } else if (operation instanceof DeleteOperation) {
            resultClass = DeleteOperation.DeleteResult.class;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    protected Result<?> logError(String message, String line, Exception e) {
        return (Result<?>) super.logError(message, line, e);
    }

    @Override
    protected Result<?> logError(String message, String line) {
        return (Result<?>) super.logError(message, line);
    }
}
