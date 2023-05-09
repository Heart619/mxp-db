package com.mxp.mdb.backend.dm.page;

import com.mxp.mdb.backend.dm.pageCache.PageCache;
import com.mxp.mdb.backend.utils.Parser;

import java.util.Arrays;

/**
 * 管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 *
 * @author mxp
 * @date 2023/4/12 20:39
 */
public class CommonPage {

    private static final short OFFSET_FREE = 0;
    private static final short OFFSET_DATA = 2;

    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OFFSET_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFreeSpaceOffset(raw, OFFSET_DATA);
        return raw;
    }

    /**
     * 修改页面空闲位置
     * @param raw
     * @param offsetData
     */
    private static void setFreeSpaceOffset(byte[] raw, short offsetData) {
        System.arraycopy(Parser.shortToByte(offsetData), 0, raw, OFFSET_FREE, OFFSET_DATA);
    }

    /**
     * 获得页面空闲位置
     * @param page
     * @return
     */
    public static short getFreeSpaceOffset(Page page) {
        return getFreeSpaceOffset(page.getData());
    }

    private static short getFreeSpaceOffset(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, OFFSET_FREE, OFFSET_DATA));
    }

    /**
     * 将raw插入page
     * 返回插入位置
     * @param page
     * @param raw
     * @return
     */
    public static short insert(Page page, byte[] raw) {
        short freeSpaceOffset = getFreeSpaceOffset(page);
        System.arraycopy(raw, 0, page.getData(), freeSpaceOffset, raw.length);
        setFreeSpaceOffset(page.getData(), (short) (freeSpaceOffset + raw.length));
        page.setDirty(true);
        return freeSpaceOffset;
    }

    /**
     * 获得页面空闲空间大小
     * @param page
     * @return
     */
    public static int getPageFreeSpace(Page page) {
        short freeSpaceOffset = getFreeSpaceOffset(page);
        return PageCache.PAGE_SIZE - freeSpaceOffset;
    }

    /**
     * 将raw插入page中的offset位置，并将page的offset设置为较大的offset
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        short curOffset = getFreeSpaceOffset(page);
        page.setDirty(true);

        if (curOffset < (curOffset = (short) (raw.length + offset))) {
            setFreeSpaceOffset(page.getData(), curOffset);
        }
    }

    /**
     *  将raw插入pg中的offset位置，不更新update
     * @param page
     * @param raw
     * @param offset
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        page.setDirty(true);
    }
}
