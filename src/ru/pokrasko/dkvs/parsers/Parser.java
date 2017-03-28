package ru.pokrasko.dkvs.parsers;

import java.io.IOException;
import java.text.ParseException;

abstract class Parser {
    protected String line = "";
    int tokenBegin;
    protected int curIndex;

    protected int parseInteger() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read an integer (" + e.getMessage() + ")", -1);
            }
        }
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
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

    long parseLong() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read a long (" + e.getMessage() + ")", -1);
            }
        }
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
        }
        tokenBegin = curIndex;

        boolean isNegative = false;
        if (currentChar() == '-') {
            isNegative = true;
            curIndex++;
        }
        if (!Character.isDigit(currentChar())) {
            throw new ParseException("Couldn't read a long (non-digit symbol)", curIndex);
        }
        int begin = curIndex;
        while (curIndex < line.length() && Character.isDigit(currentChar())) {
            curIndex++;
        }

        try {
            long result = Long.parseLong(line.substring(begin, curIndex));
            if (isNegative) {
                result *= -1;
            }
            return result;
        } catch (NumberFormatException e) {
            throw new ParseException("Couldn't read a long (" + e.getMessage() + ")", begin);
        }
    }

    byte parseByte() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read a byte integer (" + e.getMessage() + ")", -1);
            }
        }
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
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

    protected String parsePureWord() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read a word (" + e.getMessage() + ")", -1);
            }
        }
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
        }
        tokenBegin = curIndex;

        if (!Character.isAlphabetic(currentChar())) {
            throw new ParseException("Couldn't read a word (not a letter)", curIndex);
        }
        int begin = curIndex;
        while (curIndex < line.length() && Character.isJavaIdentifierPart(currentChar())) {
            curIndex++;
        }
        return line.substring(begin, curIndex);
    }

    String parseDirtyWord() throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read a word (" + e.getMessage() + ")", -1);
            }
        }
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
        }
        tokenBegin = curIndex;

        int begin = curIndex;
        while (curIndex < line.length() && !Character.isWhitespace(currentChar())) {
            curIndex++;
        }
        return line.substring(begin, curIndex);
    }

    protected String parseWordToDelimiter(char delimiter) throws ParseException {
        if (curIndex == line.length()) {
            try {
                readLine();
            } catch (IOException e) {
                throw new ParseException("Couldn't read a word (" + e.getMessage() + ")", -1);
            }
        }
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
        }
        tokenBegin = curIndex;

        int delimiterIndex = line.indexOf("" + delimiter, curIndex);
        if (delimiterIndex == -1) {
            throw new ParseException("Couldn't read a word (delimiter '" + delimiter + "' not found')", curIndex);
        }
        String word = line.substring(curIndex, delimiterIndex);
        curIndex = delimiterIndex;
        return word;
    }

    protected void readChar(char expected) throws ParseException {
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
        }
        if (curIndex == line.length()) {
            throw new ParseException("(reached end of string)", curIndex);
        } else if (currentChar() != expected) {
            throw new ParseException("(expected '" + expected + "', but got '" + currentChar() + "'", curIndex);
        }
        curIndex++;
    }

    protected void checkEnd() throws ParseException {
        while (curIndex < line.length() && Character.isWhitespace(currentChar())) {
            curIndex++;
        }
        if (curIndex != line.length()) {
            tokenBegin = curIndex;
            throw new ParseException("excess sybmols", curIndex);
        }
    }

    private char currentChar() {
        return line.charAt(curIndex);
    }

    protected Object logError(String message, String line, Exception e) {
        System.err.println(message + " (" + e.getMessage() + ") at symbol " + tokenBegin
                + " in line \"" + line + "\"");
        return null;
    }

    protected Object logError(String message, String line) {
        System.err.println(message + " at symbol " + tokenBegin + " in line \"" + line + "\"");
        return null;
    }

    protected abstract void readLine() throws IOException;
}
