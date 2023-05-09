package com.mxp.mdb.backend.server;

import com.mxp.mdb.backend.parser.ParserSql;
import com.mxp.mdb.backend.parser.statement.*;
import com.mxp.mdb.backend.tbm.BeginRes;
import com.mxp.mdb.backend.tbm.TableManager;
import com.mxp.mdb.common.error.Error;

/**
 * @author mxp
 * @date 2023/4/20 15:52
 */
public class Executor {

    private TableManager tableManager;
    private long xid;

    public Executor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tableManager.abort(xid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {
        Object stat = ParserSql.parse(sql);
        byte[] res;
        if (stat instanceof Begin) {
            if (xid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes r = tableManager.begin((Begin) stat);
            xid = r.xid;
            return r.result;
        } else if (stat instanceof Commit) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            res = tableManager.commit(xid);
            xid = 0;
            return res;
        } else if (stat instanceof Rollback) {
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            res = tableManager.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute(stat);
        }
    }

    public byte[] execute(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            tmpTransaction = true;
            BeginRes r = tableManager.begin(new Begin());
            xid = r.xid;
        }

        try {
            byte[] res = null;
            if (stat instanceof Show) {
                res = tableManager.show(xid);
            } else if (stat instanceof Create) {
                res = tableManager.create(xid, (Create) stat);
            } else if (stat instanceof Select) {
                res = tableManager.read(xid, (Select) stat);
            } else if (stat instanceof Insert) {
                res = tableManager.insert(xid, (Insert) stat);
            } else if (stat instanceof Delete) {
                res = tableManager.delete(xid, (Delete) stat);
            } else if (stat instanceof Update) {
                res = tableManager.update(xid, (Update) stat);
            }
            return res;
        } catch (Exception ex) {
            e = ex;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    tableManager.abort(xid);
                } else {
                    tableManager.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
