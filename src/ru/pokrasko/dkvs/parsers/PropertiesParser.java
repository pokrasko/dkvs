package ru.pokrasko.dkvs.parsers;

import ru.pokrasko.dkvs.Properties;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class PropertiesParser extends Parser {
    private BufferedReader reader;

    public PropertiesParser(File propertiesFile) throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(propertiesFile));
    }

    public Properties parse() throws ParseException {
        String firstWord;
        List<InetSocketAddress> serverAddresses = new ArrayList<>();
        int timeout;

        for (int i = 1; (firstWord = parsePureWord()).equals("node"); i++) {
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
        } else if (timeout > 1000000000) {
            throw new ParseException("Timeout should be less than 1000000000 but is " + timeout, tokenBegin);
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


    @Override
    void readLine() throws IOException {
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
}
