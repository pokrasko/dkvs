package ru.pokrasko.dkvs.parsers;

import java.io.IOException;

abstract class LineParser extends Parser {
    @Override
    void readLine() throws IOException {
        throw new IOException("the end of line");
    }

    void init(String line) {
        this.line = line;
        curIndex = 0;
    }

    public abstract Object parse(String line);
}
