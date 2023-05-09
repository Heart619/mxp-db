package com.mxp.mdb.backend.dm;

import com.mxp.mdb.backend.dm.dataItem.DataItem;

/**
 * @author mxp
 * @date 2023/4/13 19:04
 */
public interface DataManager {

    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void closeDataManager();

}
