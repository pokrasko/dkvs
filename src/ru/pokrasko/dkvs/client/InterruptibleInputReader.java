package ru.pokrasko.dkvs.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

class InterruptibleInputReader {
    private volatile FileChannel channel;
    private String buffer = "";
    private byte[] leftover = new byte[0];
    private boolean closed;

    InterruptibleInputReader(FileChannel channel) {
        this.channel = channel;
    }

    String readLine() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8192);
        CharBuffer charBuffer = CharBuffer.allocate(8192);
        CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();
        while (!closed && !buffer.contains("\n")) {
            try {
                byteBuffer.clear();
                byteBuffer.put(leftover);
                channel.read(byteBuffer);

                byteBuffer.flip();
                charBuffer.clear();
                charsetDecoder.reset();
                charsetDecoder.decode(byteBuffer, charBuffer, true);
                charsetDecoder.flush(charBuffer);
                leftover = new byte[byteBuffer.limit() - byteBuffer.position()];
                byteBuffer.get(leftover);

                charBuffer.flip();
                buffer += charBuffer.toString();
            } catch (IOException ignored) {
                closed = true;
                break;
            }
        }

        int newLineSymbol = buffer.indexOf("\n");
        if (newLineSymbol != -1) {
            String line = buffer.substring(0, newLineSymbol);
            buffer = buffer.substring(newLineSymbol + 1);
            return line;
        } else if (!buffer.isEmpty()) {
            String line = buffer;
            buffer = "";
            return line;
        } else {
            return null;
        }
    }

    void close() {
        try {
            channel.close();
        } catch (IOException e) {
            System.err.println("Couldn't close the input: " + e.getMessage());
        }
    }
}
