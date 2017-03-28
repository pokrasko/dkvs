package ru.pokrasko.dkvs.messages;

import ru.pokrasko.dkvs.parsers.Parsable;

public abstract class Message implements Parsable {
    private String type;

    Message(String type) {
        this.type = type;
    }

    String _toString(Object... parameters) {
        StringBuilder builder = new StringBuilder();
        builder.append(type);
        for (Object parameter : parameters) {
            builder.append(" ").append(parameter);
        }
        return builder.toString();
    }

    static <T extends Message> T construct(Class<T> thisClass, Class<?>[] classes, Object... data) {
        if (data.length != classes.length) {
            System.err.println("Message construct: wrong length, should be " + classes.length
                    + ", but is " + data.length);
            return null;
        }
        for (int i = 0; i < data.length; i++) {
            if (!classes[i].isAssignableFrom(data[i].getClass())) {
                System.err.println("Message construct: invalid argument #" + (i + 1)
                        + ", should be of type " + classes[i].getName() + ", but is of type " + data[i].getClass());
                return null;
            }
        }

        try {
            return thisClass.getConstructor(classes).newInstance(data);
        } catch (Exception e) {
            System.err.println("Couldn't construct a message: " + e.getLocalizedMessage());
            return null;
        }
    }

    @Override
    public abstract String toString();
}
