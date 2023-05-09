package com.mxp.mdb.backend.tbm;

import com.mxp.mdb.backend.parser.statement.*;

/**
 * @author mxp
 * @date 2023/4/18 16:28
 */
public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);
    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;
    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;
}
