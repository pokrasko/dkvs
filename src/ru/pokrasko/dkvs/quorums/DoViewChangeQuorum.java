package ru.pokrasko.dkvs.quorums;

import ru.pokrasko.dkvs.replica.Log;

public class DoViewChangeQuorum extends VlnkQuorum {
    private int maxViewNumberReplicaId;

    public DoViewChangeQuorum(int quorumNumber, int capacity, int thisId,
                              int viewNumber, Log log, int opNumber, int commitNumber) {
        super(quorumNumber, capacity);
        this.data = new Data(viewNumber, log, opNumber, commitNumber);
        this.maxViewNumberReplicaId = thisId;
    }

    public int getMaxViewNumberReplicaId() {
        return maxViewNumberReplicaId;
    }

    @Override
    public Boolean check() {
        return confirmedNumber >= quorumNumber;
    }

    @Override
    protected void checkVlnk(int replicaId, int viewNumber, Log log, int opNumber, int commitNumber) {
        if (viewNumber > data.viewNumber || viewNumber == data.viewNumber && opNumber > data.opNumber) {
            data.viewNumber = viewNumber;
            data.log = log;
            data.opNumber = opNumber;
            maxViewNumberReplicaId = replicaId;
        }
        if (commitNumber > data.commitNumber) {
            data.commitNumber = commitNumber;
        }
    }
}
