package ru.pokrasko.dkvs.service;

import java.util.AbstractMap;
import java.util.Map;

public class SetOperation extends Operation<Map.Entry<String, String>, Void> {
    public SetOperation(String key, String value) {
        super("set", new SpacedMapEntry<>(key, value));
    }

    @Override
    public Result<Void> initResult(Void result) {
        return new SetResult(null);
    }


    private static class SpacedMapEntry<K, V> extends AbstractMap.SimpleEntry<K, V> {
        private SpacedMapEntry(K key, V value) {
            super(key, value);
        }

        @Override
        public String toString() {
            return getKey() + " " + getValue();
        }
    }


    public static class SetResult extends Result<Void> {
        public SetResult(Void result) {
            super(result);
        }

        @Override
        public String toString() {
            return "STORED";
        }
    }
}
