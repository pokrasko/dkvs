package ru.pokrasko.dkvs.messages;

public interface ProtocolMessage {
    enum Protocol {
        NORMAL, VIEW_CHANGE, RECOVERY, STATE_TRANSFER
    }
    Protocol getProtocol();
}
