package ru.pokrasko.dkvs.files;

import ru.pokrasko.dkvs.parsers.FileParser;
import ru.pokrasko.dkvs.parsers.OperationParser;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Request;
import ru.pokrasko.dkvs.service.Operation;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class RecoveryFileHandler {
    private static final String RECOVERY_FILENAME = "recovery.txt";

    private File recoveryFile;
    private PrintWriter out;

    private RecoveryParser parser;

    public RecoveryFileHandler() {
        recoveryFile = new File(RECOVERY_FILENAME);
        try {
            parser = new RecoveryParser();
        } catch (FileNotFoundException e) {
            System.out.println("There is no recovery file, the replica will start from scratch");
            parser = null;
        }
    }

    public Integer readOpNumber() {
        return parser != null ? parser.parseOpNumber() : null;
    }

    public Log readLog() {
        return parser != null ? parser.parseLog() : null;
    }

    public void finalize(Log log, int opNumber) {}

    private class RecoveryParser extends FileParser {
        private OperationParser operationParser;

        RecoveryParser() throws FileNotFoundException {
            super(recoveryFile);
            this.operationParser = new OperationParser();
        }

        private Integer parseOpNumber() {
            try {
                String caption1 = parsePureWord();
                String caption2 = parsePureWord();
                readChar('=');
                if (caption1.equals("Operation") && caption2.equals("number")) {
                    return parseInteger();
                } else {
                    return null;
                }
            } catch (ParseException e) {
                System.err.println("Recovery parser exception at symbol " + (e.getErrorOffset() + 1) + ": "
                        + e.getMessage());
                return null;
            }
        }

        private Log parseLog() {
            List<Request<?, ?>> log = new ArrayList<>();

            while (true) {
                try {
                    readLine();
                } catch (IOException ignored) {
                    break;
                }

                int operationBegin = line.indexOf('<');
                int operationEnd = line.indexOf('>');
                Operation<?, ?> operation = operationParser.parse(line.substring(operationBegin + 1, operationEnd));
                if (operation == null) {
                    // TODO: handle operation parsing exception
                }

                curIndex = operationEnd + 1;
                int clientId;
                int requestNumber;
                try {
                    readChar('#');
                    clientId = parseInteger();
                    readChar('-');
                    requestNumber = parseInteger();
                } catch (ParseException e) {
                    // TODO: handle request id parsing exception
                    return null;
                }

                log.add(Request.fromOperation(operation, clientId, requestNumber));
            }

            return new Log(log);
        }
    }
}
