package ru.pokrasko.dkvs.files;

import ru.pokrasko.dkvs.parsers.FileParser;
import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Request;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LogFileHandler {
    private static final String RECOVERY_FILENAME = "recovery.log";

    private File logFile;
    private PrintWriter out;

    private LogFileParser parser;

//    private int logSize;

    public LogFileHandler(boolean isRecovering) throws IOException {
        this(new File(RECOVERY_FILENAME), isRecovering);
    }

    public LogFileHandler(File logFile, boolean isRecovering) throws IOException {
        this.logFile = logFile;
        this.parser = isRecovering ? new LogFileParser() : null;
        this.out = new PrintWriter(new FileWriter(logFile, isRecovering));
    }

    public Log readLog() {
        if (parser != null) {
            Log log = parser.parseLog();
            try {
                parser.close();
            } catch (IOException e) {
                System.err.println("Couldn't close the log file reader: " + e.getMessage());
            }
//            logSize = log.size();
            return log;
        } else {
            return null;
        }
    }

    public void appendRequest(Request<?, ?> request) {
        out.println(request);
//        logSize++;
    }

    public void close() {
        out.close();
        if (out.checkError()) {
            System.err.println("Couldn't close the log file writer");
        }
    }

//    public void appendLog(Log log) {
//        Log suffixToWrite = log.getAfter(logSize);
//        for (Request<?, ?> request : suffixToWrite.getList()) {
//            out.println(request);
//        }
//        logSize += suffixToWrite.size();
//    }

    private class LogFileParser extends FileParser {
        LogFileParser() throws FileNotFoundException {
            super(logFile);
        }

        private Log parseLog() {
            List<Request<?, ?>> log = new ArrayList<>();
            Request.RequestParser requestParser = new Request.RequestParser();

            while (true) {
                try {
                    readLine();
                } catch (IOException ignored) {
                    break;
                }

                Request<?, ?> request = requestParser.parse(line);
                if (request != null) {
                    log.add(request);
                }
            }

            return new Log(log);
        }
    }
}
