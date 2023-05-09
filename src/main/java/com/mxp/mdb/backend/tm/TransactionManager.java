package com.mxp.mdb.backend.tm;

import java.io.IOException;

/**
 * 事务管理器
 * @author mxp
 * @date 2023/4/6 19:22
 */
public interface TransactionManager {

    /**
     * 超级事务，永远为committed状态
     */
    long SUPER_XID = 0;

    /**
     * 开启新事务
     * @return 事务ID
     */
    long begin();

    /**
     * 提交一个事务
     * @param xid 事务ID
     */
    void commit(long xid);

    /**
     * 回滚指定事务
     * @param xid 事务ID
     */
    void rollback(long xid);

    /**
     * 查询一个事务状态是否活跃
     * @param xid 事务ID
     * @return true-活跃   false-提交/回滚
     */
    boolean isActive(long xid);

    /**
     * 查询一个事务是否提交
     * @param xid 事务ID
     * @return true-已提交  false-未提交
     */
    boolean isCommitted(long xid);

    /**
     * 查询一个事务是否回滚
     * @param xid 事务ID
     * @return true-已回滚  false-未回滚
     */
    boolean isRollback(long xid);

    /**
     * 关闭事务管理器
     * @throws IOException
     */
    void close() throws IOException;
}
