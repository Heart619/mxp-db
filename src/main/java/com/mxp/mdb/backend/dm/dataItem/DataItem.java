package com.mxp.mdb.backend.dm.dataItem;

import com.mxp.mdb.backend.common.SubArray;
import com.mxp.mdb.backend.dm.page.Page;
import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Parser;

/**
 * @author mxp
 * @date 2023/4/13 19:06
 */
public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.shortToByte((short) raw.length);
        return ArrayUtil.concat(valid, size, raw);
    }

    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OFFSET_VALID] = 1;
    }
}
