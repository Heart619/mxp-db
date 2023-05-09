package com.mxp.mdb.backend.dm.page;

import com.mxp.mdb.backend.dm.pageCache.PageCache;
import com.mxp.mdb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 * 如果是异常关闭，就需要执行数据的恢复流程。
 *
 * @author mxp
 * @date 2023/4/12 20:22
 */
public class FirstPage {

    private static final int VALID_CHECK_OFFSET = 100;
    private static final int VALID_CHECK_LEN = 8;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setValidCheckOpen(raw);
        return raw;
    }

    public static void setValidCheckOpen(Page page) {
        page.setDirty(true);
        setValidCheckOpen(page.getData());
    }

    private static void setValidCheckOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(VALID_CHECK_LEN), 0, raw, VALID_CHECK_OFFSET, VALID_CHECK_LEN);
    }

    public static void setValidCheckClose(Page page) {
        page.setDirty(true);
        setValidCheckClose(page.getData());
    }

    private static void setValidCheckClose(byte[] raw) {
        System.arraycopy(raw, VALID_CHECK_OFFSET, raw, VALID_CHECK_LEN + VALID_CHECK_OFFSET, VALID_CHECK_LEN);
    }

    public static boolean validCheckFirstPage(Page page) {
        return validCheckFirstPage(page.getData());
    }

    private static boolean validCheckFirstPage(byte[] raw) {
        return Arrays.equals(
                Arrays.copyOfRange(raw, VALID_CHECK_OFFSET, VALID_CHECK_OFFSET + VALID_CHECK_LEN),
                Arrays.copyOfRange(raw, VALID_CHECK_OFFSET + VALID_CHECK_LEN, VALID_CHECK_OFFSET + (VALID_CHECK_LEN << 1))
        );
    }
}
