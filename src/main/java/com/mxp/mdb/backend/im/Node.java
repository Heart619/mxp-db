package com.mxp.mdb.backend.im;

import com.mxp.mdb.backend.common.SubArray;
import com.mxp.mdb.backend.dm.dataItem.DataItem;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * <p>
 * LeafFlag 标记了该节点是否是个叶子节点
 * KeyNumber 为该节点中 key 的个数
 * SiblingUid 是其兄弟节点存储在 DM 中的 UID。
 * 后续是穿插的子节点（SonN）和 KeyN。最后的一个 KeyN 始终为 MAX_VALUE
 *
 * @author mxp
 * @date 2023/4/18 9:00
 */
public class Node {

    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET + 1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET + 2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET + 8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    BTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        raw.raw[raw.start + IS_LEAF_OFFSET] = isLeaf ? (byte) 1 : (byte) 0;
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte) 1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.shortToByte((short) noKeys), 0, raw.raw, raw.start + NO_KEYS_OFFSET, Short.BYTES);
    }

    static int getRawNoKeys(SubArray raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET + Short.BYTES));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.longToByte(sibling), 0, raw.raw, raw.start + SIBLING_OFFSET, Long.BYTES);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + SIBLING_OFFSET, raw.start + SIBLING_OFFSET + Long.BYTES));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        System.arraycopy(Parser.longToByte(uid), 0, raw.raw, raw.start + NODE_HEADER_SIZE + (2 * Long.BYTES * kth), Long.BYTES);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + (2 * Long.BYTES * kth);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + 8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        System.arraycopy(Parser.longToByte(key), 0, raw.raw, raw.start + NODE_HEADER_SIZE + (2 * Long.BYTES * kth) + Long.BYTES, Long.BYTES);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE + (2 * Long.BYTES * kth) + Long.BYTES;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset + Long.BYTES));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start + NODE_HEADER_SIZE + (2 * Long.BYTES * kth);
        System.arraycopy(from.raw, offset, to.raw, to.start + NODE_HEADER_SIZE, from.end - offset);
    }

    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * (8 * 2);
        int end = raw.start + NODE_SIZE - 1;
        for (int i = end; i >= begin; --i) {
            raw.raw[i] = raw.raw[i - (8 << 1)];
        }
    }

    static byte[] newRootRaw(long left, long right, long key) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);
        return raw.raw;
    }

    public static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);
        return raw.raw;
    }

    public static Node loadNode(BTree bTree, long leafUid) throws Exception {
        DataItem raw = bTree.dm.read(leafUid);
        assert raw != null;
        Node node = new Node();
        node.tree = bTree;
        node.dataItem = raw;
        node.uid = leafUid;
        node.raw = raw.data();
        return node;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    static class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 寻找对应 key 的 UID, 如果找不到, 则返回兄弟节点的 UID
     * @param key
     * @return
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for (int i = 0; i < noKeys; ++i) {
                long rawKthKey = getRawKthKey(raw, i);
                if (key < rawKthKey) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    static class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在当前节点进行范围查找，范围是 [leftKey, rightKey]
     * 这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKey = getRawNoKeys(raw);
            int kth = 0;
            while (kth < noKey) {
                if (getRawKthKey(raw, kth) >= leftKey) {
                    break;
                }
                ++kth;
            }

            List<Long> uids = new ArrayList<>();
            while (kth < noKey) {
                if (getRawKthKey(raw, kth) > rightKey) {
                    break;
                }
                uids.add(getRawKthSon(raw, kth));
                ++kth;
            }

            LeafSearchRangeRes res = new LeafSearchRangeRes();
            if (kth == noKey) {
                res.siblingUid = getRawSibling(raw);
            }
            res.uids = uids;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    static class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    static class SplitRes {
        long newSon, newKey;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }

            if (!needSplit()) {
                return res;
            }

            try {
                SplitRes r = split();
                res.newSon = r.newSon;
                res.newKey = r.newKey;
                return res;
            } catch (Exception e) {
                err = e;
                throw e;
            }
        } finally {
            if (err == null && success) {
                dataItem.after(TransactionManager.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);

        long son = tree.dm.insert(TransactionManager.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    private boolean insert(long uid, long key) {
        int noKey = getRawNoKeys(raw);
        int kth = 0;
        while (kth < noKey) {
            if (getRawKthKey(raw, kth) >= key) {
                break;
            }
            ++kth;
        }

        if (kth == noKey && getRawSibling(raw) != 0) {
            return false;
        }

        if (getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
        } else {
            long rkk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth + 1);
            setRawKthKey(raw, rkk, kth + 1);
            setRawKthSon(raw, uid, kth + 1);
        }

        setRawNoKeys(raw, noKey + 1);
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER * 2 == getRawNoKeys(raw);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for (int i = 0; i < KeyNumber; i++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
