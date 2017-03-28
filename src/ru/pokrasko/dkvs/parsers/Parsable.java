package ru.pokrasko.dkvs.parsers;

public interface Parsable {
    class Token {
        public enum Type {
            CHAR, BYTE, INTEGER, LONG, PURE_WORD, WORD_TO_DELIMITER, DIRTY_WORD, OBJECT
        }

        Type type;
        Object value;

        public Token(Type type, Object value) {
            this.type = type;
            this.value = value;
        }
    }
}
