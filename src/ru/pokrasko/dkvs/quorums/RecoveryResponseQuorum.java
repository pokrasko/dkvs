package ru.pokrasko.dkvs.quorums;

import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Request;

import java.util.List;

public class RecoveryResponseQuorum extends VlnkQuorum {
    public RecoveryResponseQuorum(int quorumNumber, int capacity) {
        super(quorumNumber, capacity);
        this.data = new Data();
    }

    @Override
    public Boolean check() {
//        return confirmedNumber >= quorumNumber && log != null;
        return confirmedNumber >= quorumNumber && data.log != null;
    }

    @Override
    protected void checkVlnk(int replicaId, int viewNumber, Log log, int opNumber, int commitNumber) {
//        if (viewNumber > this.viewNumber) {
//            this.viewNumber = viewNumber;
//            if (log != null) {
//                this.log = log;
//                this.opNumber = opNumber;
//                this.commitNumber = commitNumber;
//            } else {
//                this.log = null;
//            }
//        }
        if (viewNumber >= data.viewNumber && log != null) {
            data.log = log;
            data.opNumber = opNumber;
            data.commitNumber = commitNumber;
        }
        if (viewNumber > data.viewNumber) {
            data.viewNumber = viewNumber;
            if (log == null) {
                data.log = null;
            }
        }
    }
}
