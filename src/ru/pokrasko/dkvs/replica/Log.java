package ru.pokrasko.dkvs.replica;

import ru.pokrasko.dkvs.parsers.LineParser;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Log {
    private List<Request<?, ?>> list;

    public Log() {
        this.list = new ArrayList<>();
    }

    public Log(List<Request<?, ?>> list) {
        this.list = list;
    }

    public int size() {
        return list.size();
    }

    public Request<?, ?> get(int index) {
        return list.get(index - 1);
    }

    public void add(Request<?, ?> request) {
        list.add(request);
    }

    public void addAll(Log toAdd) {
        list.addAll(toAdd.list);
    }

    public void addAll(Log toAdd, int after, int until) {
        list.addAll(toAdd.list.subList(toAdd.list.size() + until - after, toAdd.list.size()));
    }

    public Log getAfter(int after) {
        return new Log(list.subList(after, list.size()));
    }

    public Log getSuffix(int size) {
        return new Log(list.subList(list.size() - size, size));
    }

    @Override
    public String toString() {
        return "[" + list.stream().map(Request::toString).collect(Collectors.joining(", ")) + "]";
    }

    public static class LogParser extends LineParser {
        private Request.RequestParser requestParser;

        public LogParser() {
            this.requestParser = new Request.RequestParser();
        }

        @Override
        public Log parse(String line) {
            init(line);

            List<Request<?, ?>> list = new ArrayList<>();
            try {
                while (true) {
                    try {
                        readChar('{');
                    } catch (ParseException e) {
                        break;
                    }

                    String requestLine = parseWordToDelimiter('}');
                    Request<?, ?> request = requestParser.parse(requestLine);
                    if (request == null) {
                        return null;
                    } else {
                        list.add(request);
                    }

                    readChar('}');
                }
            } catch (ParseException e) {
                return (Log) logError("Couldn't read a log", line, e);
            }

            return new Log(list);
        }
    }
}
