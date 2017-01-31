package ru.pokrasko.dkvs.parsers;

import ru.pokrasko.dkvs.messages.Message;
import ru.pokrasko.dkvs.messages.PingMessage;

import java.io.IOException;
import java.text.ParseException;

public class MessageParser extends Parser {
    public Message parse(String line) {
        this.line = line;
        curIndex = 0;

        String type;
        try {
            type = parseWord();
        } catch (ParseException e) {
            System.err.println("Couldn't read message type (" + e.getMessage() + ") in message \"" + line + "\"");
            return null;
        }

        switch (type) {
            case "ping": {
                return new PingMessage();
            } default: {
                System.err.println("Invalid message type \"" + type + "\" in message \"" + line + "\"");
                return null;
            }
        }
    }

    @Override
    void readLine() throws IOException {
        throw new IOException("the end of line");
    }
}
