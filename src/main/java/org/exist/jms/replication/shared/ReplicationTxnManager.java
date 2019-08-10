package org.exist.jms.replication.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.exist.storage.txn.Txn;

import java.util.HashSet;
import java.util.Set;

public class ReplicationTxnManager {

    private final static Logger LOG = LogManager.getLogger();

    private static Set<Long> txnIDs = new HashSet<>();

    public static void addReplicationTransaction(Txn txn){

        LOG.debug("Add transaction id {}", txn.getId());

        // Register listener for transaction so the ID can be cleanup afterwards
        txn.registerListener(new ReplicationTxnListener(txn));

        // Register ID
        synchronized (txnIDs) {
            txnIDs.add(txn.getId());
        }
    }

    public static boolean isReplicationTransaction(Txn txn){
        return txnIDs.contains(txn.getId());
    }


    public static void removeReplicationTransaction(long txnId){
        LOG.debug("Remove transaction id {}", txnId);
        synchronized (txnIDs) {
            txnIDs.remove(txnId);
        }
    }

}
