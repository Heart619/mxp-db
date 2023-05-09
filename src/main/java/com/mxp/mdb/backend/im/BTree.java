package com.mxp.mdb.backend.im;

import com.mxp.mdb.backend.common.SubArray;
import com.mxp.mdb.backend.dm.DataManager;
import com.mxp.mdb.backend.dm.dataItem.DataItem;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mxp
 * @date 2023/4/18 8:58
 */
public class BTree {

    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;

    public static long create(DataManager dm) throws Exception {
        byte[] raw = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManager.SUPER_XID, raw);
        return dm.insert(TransactionManager.SUPER_XID, Parser.longToByte(rootUid));
    }

    public static BTree load(long bootUid, DataManager dm) throws Exception {
        DataItem item = dm.read(bootUid);
        assert item != null;

        BTree tree = new BTree();
        tree.bootUid = bootUid;
        tree.dm = dm;
        tree.bootDataItem = item;
        tree.bootLock = new ReentrantLock();
        return tree;
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long left, long right) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, left);
        List<Long> uids = new ArrayList<>();
        Node leaf;
        Node.LeafSearchRangeRes res;
        while (true) {
            leaf = Node.loadNode(this, leafUid);
            res = leaf.leafSearchRange(left, right);
            leaf.release();

            uids.addAll(res.uids);
            if (res.siblingUid == 0) {
                break;
            }
            leafUid = res.siblingUid;
        }
        return uids;
    }

    static class InsertRes {
        long newNode, newKey;
    }

    public void close() {
        bootDataItem.release();
    }

    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if (res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    private InsertRes insert(long rootUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, rootUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if (isLeaf) {
            return insertAndSplit(rootUid, uid, key);
        }

        long next = searchNext(rootUid, key);
        InsertRes res = insert(next, uid, key);
        if (res.newNode != 0) {
            return insertAndSplit(rootUid, res.newNode, res.newKey);
        }
        return new InsertRes();
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes res = node.insertAndSplit(uid, key);
            node.release();

            if (res.siblingUid == 0) {
                InsertRes insertRes = new InsertRes();
                insertRes.newNode = res.newSon;
                insertRes.newKey = res.newKey;
                return insertRes;
            }

            nodeUid = res.siblingUid;
        }
    }

    private long searchLeaf(long nodeUid, long key) throws Exception {
        boolean isLeaf;
        Node node;
        while (true) {
            node = Node.loadNode(this, nodeUid);
            isLeaf = node.isLeaf();
            node.release();
            if (isLeaf) {
                return nodeUid;
            }
            nodeUid = searchNext(nodeUid, key);
        }
    }

    private long searchNext(long nodeUid, long key) throws Exception {
        Node node;
        Node.SearchNextRes res;
        while (true) {
            node = Node.loadNode(this, nodeUid);
            res = node.searchNext(key);
            node.release();
            if (res.uid != 0) {
                return res.uid;
            }
            nodeUid = res.siblingUid;
        }
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray raw = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start, raw.start + Long.BYTES));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] raw = Node.newRootRaw(left, right, rightKey);
            long uid = dm.insert(TransactionManager.SUPER_XID, raw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.longToByte(uid), 0, diRaw.raw, diRaw.start, Long.BYTES);
            bootDataItem.after(TransactionManager.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }
}
