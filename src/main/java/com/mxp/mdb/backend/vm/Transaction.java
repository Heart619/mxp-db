package com.mxp.mdb.backend.vm;

import com.mxp.mdb.backend.tm.TransactionManager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * vm对一个事务的抽象
 *
 * @author mxp
 * @date 2023/4/14 20:47
 */
public class Transaction {

    public long xid;

    /**
     * 隔离级别
     */
    public int level;
    public Set<Long> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction transaction = new Transaction();
        transaction.xid = xid;
        transaction.level = level;
        if (level != 0 && active != null) {
            transaction.snapshot = new HashSet<>();
            transaction.snapshot.addAll(active.keySet());
        }
        return transaction;
    }

    public boolean isInSnapshot(long xid) {
        if (xid == TransactionManager.SUPER_XID) {
            return false;
        }
        return snapshot.contains(xid);
    }
}
