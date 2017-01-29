package ru.pokrasko.dkvs;

import ru.pokrasko.dkvs.parsers.PropertiesParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.text.ParseException;

public class Main {
    public static void main(String[] args) {
        int id = 0;
        try {
            id = Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.err.println("Couldn't get the server id: " + e.getMessage());
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
        new Server(id - 1, properties).run();
    }

    private static PropertiesParser getPropertiesParser(File propertiesFile) {
        try {
            return new PropertiesParser(propertiesFile);
        } catch (FileNotFoundException e) {
            System.err.println("Properties file not found by path \"" + propertiesFile.getAbsolutePath() + "\"");
            return null;
        }
    }
}
