package ru.pokrasko.dkvs.service;

public class GetOperation extends Operation<String, String> {
    public GetOperation(String key) {
        super("get", key);
    }

    @Override
    public Result<String> initResult(String result) {
        return new GetResult(result);
    }

    public class GetResult extends Result<String> {
        GetResult(String result) {
            super(result);
        }

        @Override
        public String toString() {
            return (result != null)
                    ? "VALUE " + argument + " " + result
                    : "NOT_FOUND";
        }
    }
}
