package com.mxp.mdb.backend.vm;

import com.mxp.mdb.backend.tm.TransactionManager;

/**
 * @author mxp
 * @date 2023/4/16 15:04
 */
public class Visibility {

    public static final int READ_COMMITTED = 0;
    public static final int REPEATABLE_READ = 1;

    /**
     * 版本跳跃的检查
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == READ_COMMITTED) {
            return false;
        }
        long xmax = e.getXmax();
        return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(t.xid));
    }

    /**
     * 该版本是否可见
     * @param tm
     * @param t
     * @param e
     * @return
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if (t.level == READ_COMMITTED) {
            return readCommitted(tm, t, e);
        }
        return repeatableRead(tm, t, e);
    }

    /**
     * (XMIN == Ti and                 // 由Ti创建且
     *  (XMAX == NULL or               // 尚未被删除
     * ))
     * or                              // 或
     * (XMIN is commited and           // 由一个已提交的事务创建且
     *  XMIN < XID and                 // 这个事务小于Ti且
     *  XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
     *  (XMAX == NULL or               // 尚未被删除或
     *   (XMAX != Ti and               // 由其他事务删除但是
     *    (XMAX is not commited or     // 这个事务尚未提交或
     * XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
     * XMAX is in SP(Ti)               // 这个事务在Ti开始前还未提交
     * ))))
     *
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid, xmax = e.getXmax(), xmin = e.getXmin();
        if (xmin == xid && xmax == 0) {
            return true;
        }

        if (tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xid)) {
            if (xmax == 0) {
                return true;
            }

            if (xmax != xid && (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax))) {
                return true;
            }
        }
        return false;
    }

    /**
     * (XMIN == Ti and                             // 由Ti创建且
     *     XMAX == NULL                            // 还未被删除
     * )
     * or                                          // 或
     * (XMIN is commited and                       // 由一个已提交的事务创建且
     *     (XMAX == NULL or                        // 尚未删除或
     *     (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
     * ))
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid, xmax = e.getXmax(), xmin = e.getXmin();
        // 当前记录为该事务创建并且没有被删除
        if (xmin == xid && xmax == 0) {
            return true;
        }

        // 创建该记录的事务已提交
        if (tm.isCommitted(xmin)) {
            // 该纪录未被删除
            if (xmax == 0) {
                return true;
            }

            // 该记录被删除，不是当前事务删除并且删除该记录的事务未提交
            if (xmax != xid && !tm.isCommitted(xmax)) {
                return true;
            }
        }
        return false;
    }
}
