package ru.pokrasko.dkvs.parsers;

import java.io.IOException;

public abstract class LineParser extends Parser {
    @Override
    protected void readLine() throws IOException {
        throw new IOException("the end of line");
    }

    protected void init(String line) {
        this.line = line;
        curIndex = 0;
    }

    public abstract Object parse(String line);
}
