package com.mxp.mdb.backend.dm.page;

/**
 * @author mxp
 * @date 2023/4/12 18:22
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
