package com.mxp.mdb.backend.vm;

/**
 * @author mxp
 * @date 2023/4/14 18:08
 */
public interface VersionManager {

    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

}
