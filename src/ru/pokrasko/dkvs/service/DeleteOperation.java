package ru.pokrasko.dkvs.service;

public class DeleteOperation extends Operation<String, Boolean> {
    public DeleteOperation(String key) {
        super("delete", key);
    }

    @Override
    public Result<Boolean> initResult(Boolean result) {
        return new DeleteResult(result);
    }


    public static class DeleteResult extends Result<Boolean> {
        public DeleteResult(Boolean result) {
            super(result);
        }

        @Override
        public String toString() {
            return result ? "DELETED" : "NOT_FOUND";
        }
    }
}
