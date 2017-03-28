package ru.pokrasko.dkvs.parsers;

import java.io.*;

public abstract class FileParser extends Parser {
    private BufferedReader reader;

    public FileParser(File propertiesFile) throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(propertiesFile));
    }

    @Override
    protected void readLine() throws IOException {
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
