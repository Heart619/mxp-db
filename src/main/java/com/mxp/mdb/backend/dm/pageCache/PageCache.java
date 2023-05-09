package com.mxp.mdb.backend.dm.pageCache;

import com.mxp.mdb.backend.dm.page.Page;

/**
 * @author mxp
 * @date 2023/4/12 18:25
 */
public interface PageCache {

    /**
     * 每页大小8KB
     */
    int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pageNo) throws Exception;
    void close();
    void release(Page page);
    void truncateByBgno(int maxPageNo);
    int getPageNumber();
    void flushPage(Page page);


}
