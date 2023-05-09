package com.mxp.mdb.backend.dm;

import com.mxp.mdb.backend.common.SubArray;
import com.mxp.mdb.backend.dm.dataItem.DataItem;
import com.mxp.mdb.backend.dm.logger.Logger;
import com.mxp.mdb.backend.dm.page.CommonPage;
import com.mxp.mdb.backend.dm.page.Page;
import com.mxp.mdb.backend.dm.pageCache.PageCache;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.ArrayUtil;
import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.backend.utils.Parser;

import java.util.*;

/**
 * 恢复策略
 * 根据日志恢复数据
 *
 * updateLog:
 * [LogType] [XID] [UID] [OldRaw] [NewRaw]
 *
 * insertLog:
 * [LogType] [XID] [Pgno] [offset] [Raw]
 *
 * @author mxp
 * @date 2023/4/13 9:52
 */

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class LogInfo {
        byte type;
        long xid;
        int pageNumber;
        short offset;
    }

    static class InsertLogInfo extends LogInfo {
        byte[] raw;
    }

    static class UpdateLogInfo extends LogInfo {
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();

        byte[] raw;
        int maxPageNo = 0;
        LogInfo info = null;
        while ((raw = logger.next()) != null) {
            if (isInsertLog(raw)) {
                info = parseInsertLog(raw);
            } else if (isUpdateLog(raw)) {
                info = parseUpdateLog(raw);
            }

            if (info.pageNumber > maxPageNo) {
                maxPageNo = info.pageNumber;
            }
        }

        if (maxPageNo == 0) {
            maxPageNo = 1;
        }
        pageCache.truncateByBgno(maxPageNo);

        redoTransactions(tm, logger, pageCache);
        undoTransactions(tm, logger, pageCache);
    }

    private static void redoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();
        byte[] log;
        LogInfo info = null;
        List<LogInfo> list = new ArrayList<>();
        while ((log = logger.next()) != null) {
            if (isInsertLog(log)) {
                info = parseInsertLog(log);
                info.type = LOG_TYPE_INSERT;
            } else if (isUpdateLog(log)) {
                info = parseUpdateLog(log);
                info.type = LOG_TYPE_UPDATE;
            }

            if (!tm.isActive(info.xid)) {
                list.add(info);
            }
        }
        byte t;
        for (LogInfo logInfo : list) {
            if ((t = logInfo.type) == LOG_TYPE_INSERT) {
                doInsertLog(pageCache, (InsertLogInfo) logInfo, REDO);
            } else if (t == LOG_TYPE_UPDATE) {
                doUpdateLog(pageCache, (UpdateLogInfo) logInfo, REDO);
            }
        }
    }

    private static final int OFFSET_TYPE = 0;
    private static final int OFFSET_XID = OFFSET_TYPE + 1;
    private static final int OFFSET_PAGE_NO = OFFSET_XID + 8;
    private static final int OFFSET_OF = OFFSET_PAGE_NO + 4;
    private static final int OFFSET_INSERT_RAW = OFFSET_OF + 2;

    private static final int OFFSET_UPDATE_UID = OFFSET_XID + 8;

    private static final int OFFSET_UPDATE_RAW = OFFSET_UPDATE_UID + 8;

    /*
     * insertLog:
     * [LogType] [XID] [Pgno] [offset] [Raw]
     *    1        8      4       2
     *
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo logInfo = new InsertLogInfo();
        logInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OFFSET_XID, OFFSET_PAGE_NO));
        logInfo.pageNumber = Parser.parseInt(Arrays.copyOfRange(log, OFFSET_PAGE_NO, OFFSET_OF));
        logInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OFFSET_OF, OFFSET_INSERT_RAW));
        logInfo.raw = Arrays.copyOfRange(log, OFFSET_INSERT_RAW, log.length);
        return logInfo;
    }

    /*
     * updateLog:
     * [LogType] [XID] [UID] [OldRaw] [NewRaw]
     *     1       8      8
     *
     * UID
     * 低16位保存offset
     * 高32位保存pageno
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo logInfo = new UpdateLogInfo();
        logInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OFFSET_XID, OFFSET_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OFFSET_UPDATE_UID, OFFSET_UPDATE_RAW));
        logInfo.offset = (short) (uid & ((1 << 16) - 1));
        uid >>>= 32;
        logInfo.pageNumber = (int) (uid & ((1L << 32) - 1));
        int len = (log.length - OFFSET_UPDATE_RAW) / 2;
        logInfo.oldRaw = Arrays.copyOfRange(log, OFFSET_UPDATE_RAW, OFFSET_UPDATE_RAW + len);
        logInfo.newRaw = Arrays.copyOfRange(log, OFFSET_UPDATE_RAW + len, OFFSET_UPDATE_RAW + (len << 1));
        return logInfo;
    }

    private static void undoTransactions(TransactionManager tm, Logger logger, PageCache pageCache) {
        logger.rewind();
        Map<Long, List<LogInfo>> xidToLogRaw = new HashMap<>();
        byte[] raw;
        List<LogInfo> tmp;
        LogInfo info = null;
        while ((raw = logger.next()) != null) {
            if (isInsertLog(raw)) {
                info = parseInsertLog(raw);
                info.type = LOG_TYPE_INSERT;
            } else if (isUpdateLog(raw)) {
                info = parseUpdateLog(raw);
                info.type = LOG_TYPE_UPDATE;
            }

            if (tm.isActive(info.xid)) {
                if ((tmp = xidToLogRaw.get(info.xid)) == null) {
                    tmp = new ArrayList<>();
                    xidToLogRaw.put(info.xid, tmp);
                }
                tmp.add(info);
            }
        }

        byte b;
        for (Map.Entry<Long, List<LogInfo>> entry : xidToLogRaw.entrySet()) {
            List<LogInfo> logInfos = entry.getValue();
            for (int i = logInfos.size() - 1; i >= 0; --i) {
                if ((b = (info = logInfos.get(i)).type) == LOG_TYPE_INSERT) {
                    doInsertLog(pageCache, (InsertLogInfo) info, UNDO);
                } else if (b == LOG_TYPE_UPDATE) {
                    doUpdateLog(pageCache, (UpdateLogInfo) info, UNDO);
                }
            }
        }
    }

    private static void doInsertLog(PageCache pageCache, InsertLogInfo insertLogInfo, int type) {
        Page page = getPage(pageCache, insertLogInfo);

        try {
            if (type == REDO) {
                CommonPage.recoverInsert(page, insertLogInfo.raw, insertLogInfo.offset);
            } else if (type == UNDO) {
                DataItem.setDataItemRawInvalid(insertLogInfo.raw);
            }
        } finally {
            assert page != null;
            page.release();
        }
    }

    private static void doUpdateLog(PageCache pageCache, UpdateLogInfo updateLogInfo, int type) {
        byte[] raw;
        if (type == REDO) {
            raw = updateLogInfo.newRaw;
        } else {
            raw = updateLogInfo.oldRaw;
        }

        Page page = getPage(pageCache, updateLogInfo);
        try {
            CommonPage.recoverUpdate(page, raw, updateLogInfo.offset);
        } finally {
            page.release();
        }
    }

    private static Page getPage(PageCache pageCache, LogInfo info) {
        Page page = null;
        try {
            page = pageCache.getPage(info.pageNumber);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return page;
    }

    private static boolean isInsertLog(byte[] raw) {
        return raw[0] == LOG_TYPE_INSERT;
    }

    private static boolean isUpdateLog(byte[] raw) {
        return raw[0] == LOG_TYPE_UPDATE;
    }

    public static byte[] logUpdate(long xid, DataItem item) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] XID = Parser.longToByte(xid);
        byte[] uid = Parser.longToByte(item.getUid());
        byte[] oldRaw = item.getOldRaw();
        SubArray raw = item.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return ArrayUtil.concat(logType, XID, uid, oldRaw, newRaw);
    }

    public static byte[] logInsert(long xid, Page page, byte[] raw) {
        byte[] logType = {LOG_TYPE_INSERT};
        byte[] XID = Parser.longToByte(xid);
        byte[] pn = Parser.intToByte(page.getPageNumber());
        byte[] offset = Parser.shortToByte(CommonPage.getFreeSpaceOffset(page));
        return ArrayUtil.concat(logType, XID, pn, offset, raw);
    }
}
