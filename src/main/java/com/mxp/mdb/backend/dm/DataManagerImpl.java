package com.mxp.mdb.backend.dm;

import com.mxp.mdb.backend.common.AbstractCache;
import com.mxp.mdb.backend.dm.dataItem.DataItem;
import com.mxp.mdb.backend.dm.dataItem.DataItemImpl;
import com.mxp.mdb.backend.dm.logger.Logger;
import com.mxp.mdb.backend.dm.logger.LoggerImpl;
import com.mxp.mdb.backend.dm.page.CommonPage;
import com.mxp.mdb.backend.dm.page.FirstPage;
import com.mxp.mdb.backend.dm.page.Page;
import com.mxp.mdb.backend.dm.pageCache.PageCache;
import com.mxp.mdb.backend.dm.pageCache.PageCacheImpl;
import com.mxp.mdb.backend.dm.pageIndex.PageIndex;
import com.mxp.mdb.backend.dm.pageIndex.PageInfo;
import com.mxp.mdb.backend.tm.TransactionManager;
import com.mxp.mdb.backend.utils.Panic;
import com.mxp.mdb.backend.utils.Parser;
import com.mxp.mdb.common.error.Error;

import java.io.IOException;

/**
 * @author mxp
 * @date 2023/4/13 19:09
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    private TransactionManager tm;
    private PageCache pc;
    private Logger logger;
    private PageIndex pIndex;
    private Page pageOne;

    public DataManagerImpl(TransactionManager tm, PageCache pc, Logger logger) {
        super(0);
        this.tm = tm;
        this.pc = pc;
        this.logger = logger;
        pIndex = new PageIndex();
    }

    @Override
    protected DataItem getForCache(long key) throws Exception {
        short offset = (short) (key & ((1 << 16) - 1));
        key >>>= 32;
        int pageNo = (int) (key & ((1L << 32) - 1));
        Page page = pc.getPage(pageNo);
        return DataItemImpl.parseDateItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem obj) {
        obj.page().release();
    }

    /**
     * 在创建文件时初始化PageOne
     */
    void initPageOne() {
        int pageNo = pc.newPage(FirstPage.initRaw());
        assert pageNo == 1;
        try {
            pageOne = pc.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    /**
     * 在打开已有文件时时读入PageOne，并验证正确性
     */
    boolean checkPageOneOnLoad() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return FirstPage.validCheckFirstPage(pageOne);
    }

    /**
     * 初始化pageIndex
     */
    void initPageIndex() {
        int pageNumber = pc.getPageNumber();
        for (int i = 2; i <= pageNumber; ++i) {
            Page page = null;
            try {
                page = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(i, CommonPage.getPageFreeSpace(page));
            page.release();
        }
    }

    /**
     * 为xid生成update日志
     */
    public void logDataItem(long xid, DataItem item) {
        byte[] log = Recover.logUpdate(xid, item);
        logger.log(log);
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl item = (DataItemImpl) get(uid);
        if (!item.isValid()) {
            item.release();
            return null;
        }
        return item;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > CommonPage.MAX_FREE_SPACE) {
            Panic.panic(Error.DataTooLargeException);
        }

        PageInfo pageInfo = null;
        for (int i = 0; i < 5; ++i) {
            if ((pageInfo = pIndex.select(raw.length)) != null) {
                break;
            }
            int pno = pc.newPage(CommonPage.initRaw());
            pIndex.add(pno, CommonPage.MAX_FREE_SPACE);
        }

        if (pageInfo == null) {
            Panic.panic(Error.DatabaseBusyException);
        }

        Page page = null;
        int freeSize = 0;
        try {
            page = pc.getPage(pageInfo.getPageNo());
            byte[] log = Recover.logInsert(xid, page, raw);
            logger.log(log);

            short offset = CommonPage.insert(page, raw);
            page.release();
            return Parser.addressToUid(page.getPageNumber(), offset);
        } finally {
            if (page != null) {
                pIndex.add(pageInfo.getPageNo(), CommonPage.getPageFreeSpace(page));
            } else {
                pIndex.add(pageInfo.getPageNo(), freeSize);
            }
        }
    }

    @Override
    public void closeDataManager() {
        super.close();
        logger.close();
        // 正常关闭时复制校验字节
        FirstPage.setValidCheckClose(pageOne);
        pageOne.release();
        try {
            pc.close();
            tm.close();
        } catch (IOException e) {
        }
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pageCache = PageCacheImpl.create(path, mem);
        Logger logger = LoggerImpl.create(path);
        DataManagerImpl dataManager = new DataManagerImpl(tm, pageCache, logger);
        dataManager.initPageOne();
        return dataManager;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pageCache = PageCacheImpl.open(path, mem);
        Logger logger = LoggerImpl.open(path);
        DataManagerImpl dataManager = new DataManagerImpl(tm, pageCache, logger);

        // 如果上次数据库非正常关闭，进行恢复策略
        if (!dataManager.checkPageOneOnLoad()) {
            Recover.recover(tm, logger, pageCache);
        }

        // 初始化数据页索引
        dataManager.initPageIndex();
        // 重新设置数据页校验字节
        FirstPage.setValidCheckOpen(dataManager.pageOne);
        // 将校验页刷盘
        pageCache.flushPage(dataManager.pageOne);
        return dataManager;
    }
}
