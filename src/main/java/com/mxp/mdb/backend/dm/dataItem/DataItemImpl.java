package com.mxp.mdb.backend.dm.dataItem;

import com.mxp.mdb.backend.common.SubArray;
import com.mxp.mdb.backend.dm.DataManagerImpl;
import com.mxp.mdb.backend.dm.page.Page;
import com.mxp.mdb.backend.utils.Parser;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 *
 * @author mxp
 * @date 2023/4/13 19:18
 */
public class DataItemImpl implements DataItem {

    static final int OFFSET_VALID = 0;
    static final int OFFSET_SIZE = 1;
    static final int OFFSET_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dm;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, DataManagerImpl dm, long uid, Page page) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.dm = dm;
        this.uid = uid;
        this.page = page;
        ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        rLock = readWriteLock.readLock();
        wLock = readWriteLock.writeLock();
    }

    public boolean isValid() {
        return raw.raw[raw.start + OFFSET_VALID] == 0;
    }

    /**
     * 从页面的offset处解析DataItem
     * @return
     */
    public static DataItem parseDateItem(Page page, short offset, DataManagerImpl dm) {
        byte[] raw = page.getData();
        // 数据长度
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OFFSET_SIZE, offset + DataItemImpl.OFFSET_DATA));
        // DataItem长度
        short len = (short) (size + DataItemImpl.OFFSET_DATA);
        long uid = Parser.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + len), new byte[len], dm, uid, page);
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OFFSET_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
        page.setDirty(true);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
