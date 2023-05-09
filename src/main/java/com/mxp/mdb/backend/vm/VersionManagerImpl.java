package com.mxp.mdb.backend.vm;

import com.mxp.mdb.backend.common.AbstractCache;
import com.mxp.mdb.backend.dm.DataManager;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.common.error.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mxp
 * @date 2023/4/14 18:11
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Lock lock;
    Map<Long, Transaction> activeTransaction;
    LockTable lockTable;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManager.SUPER_XID, Transaction.newTransaction(TransactionManager.SUPER_XID, 0, null));
        lock = new ReentrantLock();
        lockTable = new LockTable();
    }

    /**
     * 读取一个 entry，注意判断下可见性即可
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        Transaction transaction;
        lock.lock();
        try {
            transaction = activeTransaction.get(xid);
        } finally {
            lock.unlock();
        }
        if (transaction.err != null) {
            throw transaction.err;
        }

        Entry entry = get(uid);
        try {
            if (Visibility.isVisible(tm, transaction, entry)) {
                return entry.data();
            }
            return null;
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        Transaction transaction;
        lock.lock();
        try {
            transaction = activeTransaction.get(xid);
        } finally {
            lock.unlock();
        }

        if (transaction.err != null) {
            throw transaction.err;
        }

        return dm.insert(xid, Entry.wrapEntryRaw(xid, data));
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        Transaction transaction;
        lock.lock();
        try {
            transaction = activeTransaction.get(xid);
        } finally {
            lock.unlock();
        }

        if (transaction.err != null) {
            throw transaction.err;
        }
        Entry entry = get(uid);
        try {
            if (!Visibility.isVisible(tm, transaction, entry)) {
                return false;
            }

            try {
                lockTable.add(xid, uid);
            } catch (Exception e) {
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }

            if (entry.getXmax() == xid) {
                return false;
            }

            if (Visibility.isVersionSkip(tm, transaction, entry)) {
                transaction.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }
            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    /**
     * 开启一个事务，并初始化事务的结构，将其存放在 activeTransaction 中，用于检查和快照使用：
     * @param level
     * @return
     */
    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction transaction = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, transaction);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 方法提交一个事务，主要就是 free 掉相关的结构，并且释放持有的锁，并修改 TM 状态：
     * @param xid
     * @throws Exception
     */
    @Override
    public void commit(long xid) throws Exception {
        Transaction transaction;
        lock.lock();
        try {
            transaction = activeTransaction.get(xid);
        } finally {
            lock.unlock();
        }

        try {
            if (transaction.err != null) {
                throw transaction.err;
            }
        } catch (NullPointerException e) {
            Panic.panic(e);
        }

        lock.lock();
        try {
            activeTransaction.remove(xid);
        } finally {
            lock.unlock();
        }
        lockTable.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry obj) {
        obj.remove();
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    /**
     * abort 事务的方法则有两种，手动和自动。<br>
     * 手动指的是调用 abort() 方法 <br>
     * 自动则是在事务被检测出出现死锁时，会自动撤销回滚事务；或者出现版本跳跃时，也会自动回滚：
     */
    private void internAbort(long xid, boolean autoAbort) {
        Transaction transaction;
        lock.lock();
        try {
            transaction = activeTransaction.get(xid);
            if (!autoAbort) {
                activeTransaction.remove(xid);
            }
        } finally {
            lock.unlock();
        }

        if (transaction.autoAborted) {
            return;
        }
        lockTable.remove(xid);
        tm.rollback(xid);
    }
}
