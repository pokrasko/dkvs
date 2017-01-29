package ru.pokrasko.dkvs.parsers;

import ru.pokrasko.dkvs.Properties;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class PropertiesParser {
    private BufferedReader reader;
    private String line = "";
    private int tokenBegin;
    private int curIndex;

    public PropertiesParser(File propertiesFile) throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(propertiesFile));
    }

    public Properties parse() throws ParseException {
        String firstWord;
        List<InetSocketAddress> serverAddresses = new ArrayList<>();
        int timeout;

        for (int i = 1; (firstWord = parseWord()).equals("node"); i++) {
            try {
                readChar('.');
                int id = parseInteger();
                if (id != i) {
                    throw new ParseException("(server id expected " + i + " but got " + id, tokenBegin);
                }
                readChar('=');
            } catch (ParseException e) {
                throw new ParseException("Couldn't read line title " + e.getMessage(), e.getErrorOffset());
            }

            serverAddresses.add(parseServerAddress());
        }

        try {
            if (!firstWord.equals("timeout")) {
                throw new ParseException("(expected timeout but got " + firstWord + ")", tokenBegin);
            }
            readChar('=');
        } catch (ParseException e) {
            throw new ParseException("Couldn't read line title " + e.getMessage(), e.getErrorOffset());
        }
        try {
            timeout = parseInteger();
        } catch (ParseException e) {
            throw new ParseException("Couldn't read timeout " + e.getMessage(), e.getErrorOffset());
        }
        if (timeout <= 0) {
            throw new ParseException("Timeout should be larger than 0 but is " + timeout, tokenBegin);
        }

        return new Properties(serverAddresses, timeout);
    }

    private InetSocketAddress parseServerAddress() throws ParseException {
        InetAddress serverIp = parseServerIp();

        try {
            readChar(':');
        } catch (ParseException e) {
            throw new ParseException("Couldn't read a server address " + e.getMessage(), e.getErrorOffset());
        }

        int port = parseInteger();
        try {
            return new InetSocketAddress(serverIp, port);
        } catch (IllegalArgumentException e) {
            throw new ParseException("Couldn't read a server address "
                    + "(port should be between 0 and 65535 but is " + port + ")", tokenBegin);
        }
    }

    private InetAddress parseServerIp() throws ParseException {
        byte[] octets = new byte[4];
        octets[0] = parseByte();

        for (int i = 1; i < 4; i++) {
            try {
                readChar('.');
            } catch (ParseException e) {
                throw new ParseException("Couldn't read an IP address " + e.getMessage(), e.getErrorOffset());
            }

            octets[i] = parseByte();
        }

        try {
            return InetAddress.getByAddress(octets);
        } catch (UnknownHostException e) {
            throw new IllegalStateException();
        }
    }


    private int parseInteger() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read an integer (" + e.getMessage() + ")", -1);
            }
        }
        tokenBegin = curIndex;

        boolean isNegative = false;
        if (currentChar() == '-') {
            isNegative = true;
            curIndex++;
        }
        if (!Character.isDigit(currentChar())) {
            throw new ParseException("Couldn't read an integer (non-digit symbol)", curIndex);
        }
        int begin = curIndex;
        while (curIndex < line.length() && Character.isDigit(currentChar())) {
            curIndex++;
        }

        try {
            int result = Integer.parseInt(line.substring(begin, curIndex));
            if (isNegative) {
                result *= -1;
            }
            return result;
        } catch (NumberFormatException e) {
            throw new ParseException("Couldn't read an integer (" + e.getMessage() + ")", begin);
        }
    }

    private byte parseByte() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read a byte integer (" + e.getMessage() + ")", -1);
            }
        }
        tokenBegin = curIndex;

        if (!Character.isDigit(currentChar())) {
            throw new ParseException("Couldn't read a byte integer (non-digit symbol)", curIndex);
        }
        int begin = curIndex;
        while (curIndex < line.length() && Character.isDigit(currentChar())) {
            curIndex++;
        }

        try {
            return Byte.parseByte(line.substring(begin, curIndex));
        } catch (NumberFormatException e) {
            throw new ParseException("Couldn't read a byte integer (" + e.getMessage() + ")", begin);
        }
    }

    private String parseWord() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read a word (" + e.getMessage() + ")", -1);
            }
        }
        tokenBegin = curIndex;

        if (!Character.isAlphabetic(currentChar())) {
            throw new ParseException("Couldn't read a byte integer (not a letter)", curIndex);
        }
        int begin = curIndex;
        while (curIndex < line.length() && Character.isAlphabetic(currentChar())) {
            curIndex++;
        }
        return line.substring(begin, curIndex);
    }

    private void readChar(char expected) throws ParseException {
        if (currentChar() != expected) {
            throw new ParseException("(expected '" + expected + "', but got '" + currentChar() + "'", curIndex);
        }
        curIndex++;
    }


    private void readLine() throws IOException {
        try {
            line = reader.readLine();
            if (line == null) {
                throw new IOException("end of file");
            } else if (line.equals("")) {
                throw new IOException("got empty line");
            }
        } catch (IOException e) {
            throw new IOException("couldn't read a line");
        }
        curIndex = 0;
    }

    private char currentChar() {
        return line.charAt(curIndex);
    }
}
