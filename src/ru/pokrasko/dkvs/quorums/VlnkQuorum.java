package ru.pokrasko.dkvs.quorums;

import ru.pokrasko.dkvs.replica.Log;
import ru.pokrasko.dkvs.replica.Request;

import java.util.List;

public abstract class VlnkQuorum extends BooleanQuorum {
    Data data;

    VlnkQuorum(int quorumNumber, int capacity) {
        super(quorumNumber, capacity);
    }

    public Data check(int replicaId, int viewNumber, Log log, int opNumber, int commitNumber) {
        checkVlnk(replicaId, viewNumber, log, opNumber, commitNumber);
        return check(replicaId, true) ? data : null;
    }

    protected abstract void checkVlnk(int replicaId,
                                      int viewNumber, Log log, int opNumber, int commitNumber);

    public class Data {
        protected int viewNumber;
        protected Log log;
        protected int opNumber;
        protected int commitNumber;

        Data() {}

        Data(int viewNumber, Log log, int opNumber, int commitNumber) {
            this.viewNumber = viewNumber;
            this.log = log;
            this.opNumber = opNumber;
            this.commitNumber = commitNumber;
        }

        public int getViewNumber() {
            return viewNumber;
        }

        public Log getLog() {
            return log;
        }

        public int getOpNumber() {
            return opNumber;
        }

        public int getCommitNumber() {
            return commitNumber;
        }
    }
}
