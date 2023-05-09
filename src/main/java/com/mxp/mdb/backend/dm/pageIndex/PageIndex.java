package com.mxp.mdb.backend.dm.pageIndex;

import com.mxp.mdb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author mxp
 * @date 2023/4/13 18:51
 */
public class PageIndex {

    /**
     * 将一页划成40个区间
     */
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex() {
        lists = new List[INTERVALS_NO + 1];
        for (int i = 0; i < lists.length; ++i) {
            lists[i] = new ArrayList<>();
        }
        lock = new ReentrantLock();
    }

    public void add(int pageNo, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNo, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            if (number < INTERVALS_NO) {
                ++number;
            }

            while (number <= INTERVALS_NO) {
                if (lists[number].isEmpty()) {
                    ++number;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
