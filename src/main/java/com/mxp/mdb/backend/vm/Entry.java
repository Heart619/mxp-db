package com.mxp.mdb.backend.vm;

import com.mxp.mdb.backend.common.SubArray;
import com.mxp.mdb.backend.dm.dataItem.DataItem;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出Entry
 * Entry结构：
 * [XMIN] [XMAX] [DATA]
 * XMIN 是创建该条记录（版本）的事务编号
 * XMAX 则是删除该条记录（版本）的事务编号
 * DATA 就是这条记录持有的数据。
 *
 * @author mxp
 * @date 2023/4/14 18:05
 */
public class Entry {

    private static final int OFFSET_XMIN = 0;
    private static final int OFFSET_XMAX = OFFSET_XMIN + 8;
    private static final int OFFSET_DATA = OFFSET_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem item = ((VersionManagerImpl) vm).dm.read(uid);
        return newEntry(vm, item, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] raw) {
        byte[] xmin = Parser.longToByte(xid);
        byte[] xmax = Parser.longToByte(TransactionManager.SUPER_XID);
        return ArrayUtil.concat(xmin, xmax, raw);
    }

    public void release() {
        ((VersionManagerImpl) vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    /**
     * 以拷贝的形式返回内容
     * @return
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray subArray = dataItem.data();
            byte[] data = new byte[subArray.end - subArray.start - OFFSET_DATA];
            System.arraycopy(subArray.raw, subArray.start + OFFSET_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray raw = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + OFFSET_XMIN, raw.start + OFFSET_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray raw = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start + OFFSET_XMAX, raw.start + OFFSET_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray raw = dataItem.data();
            byte[] bytes = Parser.longToByte(xid);
            System.arraycopy(bytes, 0, raw.raw, raw.start + OFFSET_XMAX, bytes.length);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }
}
