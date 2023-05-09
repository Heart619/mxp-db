package com.mxp.mdb.backend.dm.pageIndex;

/**
 * @author mxp
 * @date 2023/4/13 18:54
 */
public class PageInfo {

    private int pageNo;
    private int freeSpace;

    public PageInfo(int pageNo, int freeSpace) {
        this.freeSpace = freeSpace;
        this.pageNo = pageNo;
    }

    public int getPageNo() {
        return pageNo;
    }

    public int getFreeSpace() {
        return freeSpace;
    }
}
