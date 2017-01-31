package ru.pokrasko.dkvs;

import ru.pokrasko.dkvs.parsers.PropertiesParser;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.text.ParseException;

public class Main {
    public static final int ACCEPT_TIMEOUT = 1000;

    private static Server server;

    public static void main(String[] args) {
        ThisSignalHandler.install("INT");
        ThisSignalHandler.install("TERM");

        int id;
        try {
            id = Integer.parseInt(args[0]) - 1;
        } catch (Exception e) {
            System.err.println("Couldn't get the server id: " + e.getMessage());
            return;
        }

        PropertiesParser propertiesParser;
        if (args.length > 1 && args[1] != null) {
            File propertiesFile = new File(args[1]);
            propertiesParser = getPropertiesParser(propertiesFile);
        } else {
            URL propertiesUrl = Main.class.getClassLoader().getResource("dkvs.properties");
            if (propertiesUrl == null) {
                System.err.println("Resource not found: dkvs.properties");
                return;
            }
            File propertiesFile = new File(propertiesUrl.getFile());
            propertiesParser = getPropertiesParser(propertiesFile);
        }
        if (propertiesParser == null) {
            return;
        }

        Properties properties;
        try {
            properties = propertiesParser.parse();
        } catch (ParseException e) {
            System.err.println("Properties parser exception at symbol " + (e.getErrorOffset() + 1) + ": "
                    + e.getMessage());
            return;
        }

        server = new Server(id, properties);
        server.start();
    }

    private static PropertiesParser getPropertiesParser(File propertiesFile) {
        try {
            return new PropertiesParser(propertiesFile);
        } catch (FileNotFoundException e) {
            System.err.println("Properties file not found by path \"" + propertiesFile.getAbsolutePath() + "\"");
            return null;
        }
    }

    private static class ThisSignalHandler implements SignalHandler {
        private SignalHandler oldHandler;

        private static ThisSignalHandler install(String signalName) {
            Signal signal = new Signal(signalName);
            ThisSignalHandler handler = new ThisSignalHandler();
            handler.oldHandler = Signal.handle(signal, handler);
            return handler;
        }

        @Override
        public void handle(Signal signal) {
            server.stop();
            System.exit(0);
        }
    }
}
