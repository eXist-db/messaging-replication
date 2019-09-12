package org.exist.jms.replication.shared;

import org.exist.storage.txn.Txn;
import org.exist.storage.txn.TxnListener;

/**
 * Helper class to remove transaction id when a transaction is finished.
 */
public class ReplicationTxnListener implements TxnListener {

    private final long txnId;

    public ReplicationTxnListener(final Txn txn){
        txn.registerListener(this);
        this.txnId =txn.getId();
    }


    @Override
    public void commit() {
        ReplicationTxnManager.removeReplicationTransaction(txnId);
    }

    @Override
    public void abort() {
        ReplicationTxnManager.removeReplicationTransaction(txnId);
    }
}
