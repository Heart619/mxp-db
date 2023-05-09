package com.mxp.mdb.backend.vm;

import com.mxp.mdb.common.error.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mxp
 * @date 2023/4/16 15:48
 */
public class LockTable {

    /**
     * 某个XID已经获得的资源的UID列表
     */
    private Map<Long, List<Long>> xid2uids;

    /**
     * UID被某个XID持有
     */
    private Map<Long, Long> uid2xid;

    /**
     * 正在等待UID的XID列表
     */
    private Map<Long, List<Long>> uidWait2xids;

    /**
     * 正在等待资源的XID的线程
     */
    private Map<Long, Thread> xid2waitThread;

    /**
     * XID正在等待的UID
     */
    private Map<Long, Long> xid2waitUid;

    private Lock lock;

    public LockTable() {
        xid2uids = new HashMap<>();
        uid2xid = new HashMap<>();
        uidWait2xids = new HashMap<>();
        xid2waitThread = new HashMap<>();
        xid2waitUid = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 不需要等待则返回null，否则返回锁对象
     * 会造成死锁则抛出异常
     * @param xid
     * @param uid
     * @throws Exception
     */
    public void add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 当前事务已经获取该资源
            if (isInList(xid2uids, xid, uid)) {
                return;
            }
            // uid未被其它事务占用
            if (!uid2xid.containsKey(uid)) {
                uid2xid.put(uid, xid);
                putIntoList(xid2uids, xid, uid);
                return;
            }
            // uid被其它事务占用
            // 向有向图中加入一条边
            xid2waitUid.put(xid, uid);
            putIntoList(uidWait2xids, uid, xid);
            // 检测是否会发生死锁
            if (hasDeadLock()) {
                xid2waitUid.remove(xid);
                removeFromList(uidWait2xids, uid, xid);
                // 发生死锁
                throw Error.DeadlockException;
            }
            xid2waitThread.put(xid, Thread.currentThread());
            LockSupport.park();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 在一个事务 commit 或者 rollback 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> x2u = xid2uids.get(xid);
            if (x2u != null) {
                while (!x2u.isEmpty()) {
                    Long uid = x2u.remove(0);
                    selectNewXid(uid);
                }
            }
            xid2waitUid.remove(xid);
            xid2uids.remove(xid);
            xid2waitThread.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     * while循环释放掉了这个线程所有持有的资源的锁，这些资源可以被等待的线程所获取：
     * 从 List开头开始尝试解锁，还是个公平锁。解锁时，将该 Lock 对象 unlock 即可，这样业务线程就获取到了锁，就可以继续执行了。
     *
     * @param uid
     */
    private void selectNewXid(Long uid) {
        uid2xid.remove(uid);
        List<Long> xids = uidWait2xids.get(uid);
        if (xids == null || xids.isEmpty()) {
            return;
        }
        while (!xids.isEmpty()) {
            Long xid = xids.remove(0);
            Thread thread;
            if ((thread = xid2waitThread.remove(xid)) != null) {
                uid2xid.put(uid, xid);
                xid2waitUid.remove(xid);
                LockSupport.unpark(thread);
                break;
            }
        }
        if (xids.isEmpty()) {
            uidWait2xids.remove(uid);
        }
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        Integer i;
        xidStamp = new HashMap<>();
        stamp = 1;
        for (long xid : xid2uids.keySet()) {
            if ((i = xidStamp.get(xid)) != null && i > 0) {
                continue;
            }
            ++stamp;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if (stp != null && stp == stamp) {
            return true;
        }
        if (stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = xid2waitUid.get(xid);
        if (uid == null) {
            return false;
        }
        Long x = uid2xid.get(uid);
        if (x == null) {
            return false;
        }
        return dfs(x);
    }


    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null || list.isEmpty()) {
            return;
        }

        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().equals(uid1)) {
                iterator.remove();
                break;
            }
        }

        if (list.isEmpty()) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null) {
            listMap.put(uid0, (list = new LinkedList<>()));
        }
        list.add(uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> list = listMap.get(uid0);
        if (list == null || list.isEmpty()) {
            return false;
        }

        for (Long x : list) {
            if (x.equals(uid1)) {
                return true;
            }
        }
        return false;
    }
}
